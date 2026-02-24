import json
import math
import os
import re
import sys
import time
from pathlib import Path

import pandas as pd
import requests


INPUT_XLSX = "ClubLocations.xlsx"
OUTPUT_JSON = "clubs.json"
OUTPUT_REPORT = "geocode_report.csv"

# OpenStreetMap Nominatim usage policy: keep it slow (≈1 req/sec) and identify your app.
USER_AGENT = "vasa-distance-map (internal demo) - contact: shelby.ingram@vasafitness.com"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
REQUEST_DELAY_SECONDS = 1.1  # be polite

def slugify(text: str) -> str:
    text = (text or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-") or "club"

def normalize_address(addr: str) -> str:
    return re.sub(r"\s+", " ", (addr or "").strip())

def nominatim_geocode(address: str) -> tuple[float | None, float | None, str]:
    params = {
        "q": address,
        "format": "json",
        "limit": 1,
        "addressdetails": 0,
    }
    r = requests.get(
        NOMINATIM_URL,
        params=params,
        headers={"User-Agent": USER_AGENT},
        timeout=30,
    )
    if not r.ok:
        return None, None, f"HTTP {r.status_code}"
    data = r.json()
    if not data:
        return None, None, "No result"
    try:
        lat = float(data[0]["lat"])
        lon = float(data[0]["lon"])
        return lat, lon, "OK"
    except Exception as e:
        return None, None, f"Parse error: {e}"

def main():
    xlsx_path = Path(INPUT_XLSX)
    if not xlsx_path.exists():
        print(f"ERROR: {INPUT_XLSX} not found in repo root.", file=sys.stderr)
        sys.exit(1)

    df = pd.read_excel(xlsx_path)

    # Expect columns: CLUB, ADDRESS (as in your upload)
    if "CLUB" not in df.columns or "ADDRESS" not in df.columns:
        print(f"ERROR: Expected columns CLUB and ADDRESS. Found: {list(df.columns)}", file=sys.stderr)
        sys.exit(1)

    clubs_out = []
    report_rows = []

    # Simple in-run cache (also persisted to a json file to speed reruns)
    cache_file = Path("geocode_cache.json")
    cache = {}
    if cache_file.exists():
        try:
            cache = json.loads(cache_file.read_text(encoding="utf-8"))
        except Exception:
            cache = {}

    for i, row in df.iterrows():
        name = str(row.get("CLUB", "")).strip()
        address = normalize_address(str(row.get("ADDRESS", "")).strip())

        if not name or not address or address.lower() == "nan":
            report_rows.append({
                "row": i,
                "club": name,
                "address": address,
                "status": "Skipped (missing name/address)",
                "lat": "",
                "lng": "",
            })
            continue

        cache_key = address
        if cache_key in cache:
            lat, lng = cache[cache_key].get("lat"), cache[cache_key].get("lng")
            status = "OK (cached)"
        else:
            lat, lng, status = nominatim_geocode(address)
            cache[cache_key] = {"lat": lat, "lng": lng, "status": status}
            time.sleep(REQUEST_DELAY_SECONDS)

        clubs_out.append({
            "id": slugify(name),
            "name": name,
            "address": address,
            "lat": lat,
            "lng": lng,
        })

        report_rows.append({
            "row": i,
            "club": name,
            "address": address,
            "status": status,
            "lat": "" if lat is None else lat,
            "lng": "" if lng is None else lng,
        })

    # Save outputs
    Path(OUTPUT_JSON).write_text(json.dumps(clubs_out, indent=2), encoding="utf-8")
    pd.DataFrame(report_rows).to_csv(OUTPUT_REPORT, index=False)
    cache_file.write_text(json.dumps(cache, indent=2), encoding="utf-8")

    # Fail the build if any clubs have no coordinates (forces us to fix)
    missing = [c for c in clubs_out if c["lat"] is None or c["lng"] is None]
    print(f"Generated {OUTPUT_JSON} with {len(clubs_out)} clubs.")
    print(f"Missing coords: {len(missing)} (see {OUTPUT_REPORT})")
    if missing:
        sys.exit(2)

if __name__ == "__main__":
    main()
