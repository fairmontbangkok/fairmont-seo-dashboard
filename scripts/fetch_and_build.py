"""
fetch_and_build.py
─────────────────────────────────────────────────────────────────────────────
Pulls live data from Supermetrics (Google Ads + Facebook Ads) and
injects it into index.html — called daily by GitHub Actions.
─────────────────────────────────────────────────────────────────────────────
"""

import os
import json
import requests
import time
from datetime import datetime, timedelta, timezone

# ── CONFIG ───────────────────────────────────────────────────────────────────
API_KEY    = os.environ.get("SUPERMETRICS_API_KEY", "")
BASE_URL   = "https://api.supermetrics.com/enterprise/v2/query/data/json"
GA_ACCT    = os.environ.get("GOOGLE_ADS_ACCOUNT", "")
FA_ACCT    = os.environ.get("FACEBOOK_ADS_ACCOUNT", "")

BKK_TZ     = timezone(timedelta(hours=7))
TODAY      = datetime.now(BKK_TZ).strftime("%Y-%m-%d")
WEEK_AGO   = (datetime.now(BKK_TZ) - timedelta(days=7)).strftime("%Y-%m-%d")
MONTH_AGO  = (datetime.now(BKK_TZ) - timedelta(days=30)).strftime("%Y-%m-%d")
WEEK_LABEL = datetime.now(BKK_TZ).strftime("%-d %b %Y")

# ── SUPERMETRICS QUERY ───────────────────────────────────────────────────────
def query(ds_id, fields, accounts, start, end, settings=None, max_rows=100):
    """Fire a Supermetrics query and return rows list."""
    payload = {
        "json": json.dumps({
            "ds_id": ds_id,
            "ds_accounts": accounts,
            "fields": fields,
            "start_date": start,
            "end_date": end,
            "max_rows": max_rows,
            **(settings or {}),
        })
    }
    headers = {"Authorization": f"Bearer {API_KEY}"}
    try:
        r = requests.post(BASE_URL, data=payload, headers=headers, timeout=30)
        r.raise_for_status()
        resp = r.json()
        # Supermetrics may return async schedule_id
        if "data" in resp:
            return resp["data"].get("rows", [])
        # poll if async
        if "schedule_id" in resp:
            sid = resp["schedule_id"]
            for _ in range(20):
                time.sleep(3)
                poll = requests.get(
                    f"https://api.supermetrics.com/enterprise/v2/query/data/{sid}",
                    headers=headers, timeout=20
                )
                pr = poll.json()
                if pr.get("status") == "completed":
                    return pr.get("data", {}).get("rows", [])
    except Exception as e:
        print(f"  Supermetrics error [{ds_id}]: {e}")
    return []


# ── FETCH GOOGLE ADS ─────────────────────────────────────────────────────────
def fetch_google_ads():
    print("Fetching Google Ads (last 30 days)…")
    if not GA_ACCT:
        print("  No GA account — skipping")
        return {}

    rows = query(
        ds_id    = "AW",
        fields   = ["impressions", "clicks", "cost", "conversions", "ctr"],
        accounts = GA_ACCT,
        start    = MONTH_AGO,
        end      = TODAY,
    )
    if not rows:
        return {}

    # rows = [[impressions, clicks, cost, conversions, ctr], ...]
    totals = {"impressions":0, "clicks":0, "cost":0.0, "conversions":0, "ctr":0.0}
    for r in rows:
        try:
            totals["impressions"]  += int(r[0] or 0)
            totals["clicks"]       += int(r[1] or 0)
            totals["cost"]         += float(r[2] or 0)
            totals["conversions"]  += float(r[3] or 0)
            totals["ctr"]          += float(r[4] or 0)
        except (IndexError, ValueError):
            pass
    if rows:
        totals["ctr"] = totals["ctr"] / len(rows)
    print(f"  Google Ads: {totals}")
    return totals


# ── FETCH FACEBOOK ADS ───────────────────────────────────────────────────────
def fetch_facebook_ads():
    print("Fetching Facebook Ads (last 30 days)…")
    if not FA_ACCT:
        print("  No FA account — skipping")
        return {}

    rows = query(
        ds_id    = "FA",
        fields   = ["impressions", "clicks", "spend", "actions"],
        accounts = FA_ACCT,
        start    = MONTH_AGO,
        end      = TODAY,
    )
    if not rows:
        return {}

    totals = {"impressions":0, "clicks":0, "spend":0.0}
    for r in rows:
        try:
            totals["impressions"] += int(r[0] or 0)
            totals["clicks"]      += int(r[1] or 0)
            totals["spend"]       += float(r[2] or 0)
        except (IndexError, ValueError):
            pass
    print(f"  Facebook Ads: {totals}")
    return totals


# ── FORMAT HELPERS ────────────────────────────────────────────────────────────
def fmt_num(n):
    n = int(n)
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n/1_000:.1f}K"
    return str(n)

def fmt_currency(n):
    n = float(n)
    if n >= 1_000_000:
        return f"${n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"${n/1_000:.0f}K"
    return f"${n:.0f}"

def fmt_pct(n):
    return f"{float(n)*100:.2f}%"


# ── BUILD DATA JSON ───────────────────────────────────────────────────────────
def build_data(ga, fa):
    return {
        "updated":    datetime.now(BKK_TZ).strftime("%d %b %Y %H:%M"),
        "week_label": f"Week of {WEEK_LABEL}",
        "google_ads": {
            "impressions":  fmt_num(ga.get("impressions", 0)),
            "clicks":       fmt_num(ga.get("clicks", 0)),
            "cost":         fmt_currency(ga.get("cost", 0)),
            "conversions":  fmt_num(ga.get("conversions", 0)),
            "ctr":          fmt_pct(ga.get("ctr", 0)),
        },
        "facebook_ads": {
            "impressions": fmt_num(fa.get("impressions", 0)),
            "clicks":      fmt_num(fa.get("clicks", 0)),
            "spend":       fmt_currency(fa.get("spend", 0)),
        },
        "seo": {
            # Static SEO data — update manually each Monday
            # or connect Google Search Console API here
            "title_tag_score":    "6/10",
            "pagespeed_mobile":   "~52",
            "schema_score":       "3/10",
            "llm_visibility":     "8/100",
        }
    }


# ── INJECT INTO HTML ──────────────────────────────────────────────────────────
def inject_into_html(data):
    with open("index.html", "r", encoding="utf-8") as f:
        html = f.read()

    # Replace date stamp
    import re
    html = re.sub(
        r'Week of <span id="wk-date">[^<]*</span>',
        f'Week of <span id="wk-date">{data["week_label"].replace("Week of ","")}</span>',
        html
    )
    html = re.sub(
        r'Updated <span id="upd-time">[^<]*</span>',
        f'Updated <span id="upd-time">{data["updated"]}</span>',
        html
    )

    # Inject Google Ads KPIs if available
    if data["google_ads"]["impressions"] != "0":
        ga = data["google_ads"]
        # Replace the static "NEW" organic visibility card with real paid data card note
        # We add a live data banner inside the topbar badge area
        html = html.replace(
            '<span class="badge">Search Console + Manual SERP</span>',
            f'<span class="badge">Search Console + Manual SERP</span>'
            f'<span class="badge" style="background:#e2f5ec;color:#065f46;border-color:#9FE1CB">'
            f'Google Ads: {ga["cost"]} spend · {ga["clicks"]} clicks</span>'
        )

    with open("index.html", "w", encoding="utf-8") as f:
        f.write(html)

    print(f"  index.html updated — {len(html):,} bytes")


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"\n{'='*60}")
    print(f"Fairmont Bangkok SEO Dashboard — Daily Refresh")
    print(f"Bangkok time: {datetime.now(BKK_TZ).strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}\n")

    ga_data = fetch_google_ads()
    fa_data = fetch_facebook_ads()

    data = build_data(ga_data, fa_data)

    # Save raw data as JSON (useful for debugging / future use)
    with open("data.json", "w") as f:
        json.dump(data, f, indent=2)
    print(f"\ndata.json saved")

    inject_into_html(data)

    print(f"\nDone. Dashboard refreshed at {data['updated']} BKK time.\n")

