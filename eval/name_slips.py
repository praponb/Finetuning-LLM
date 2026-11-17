"""Count name slips in evaluation results: failures with an unresolved table / column / struct-field name - a name
the SQL does not have, e.g. v13 `df_patents`, v14 `apppt_date` / `num_stores` / `ss.restock`, or an invented helper
column such as v13 `s2.avg_sqft_by_city` - as opposed to logic errors (wrong rows, wrong columns, syntax).

  python eval/name_slips.py --models v12-q4 v14-q4 v15-q4
      -> a markdown table: per model and eval set, failures and how many of them are name slips
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "eval" / "results"
# Spark / Python messages for a name that does not exist
NAME_ERR = re.compile(r"UNRESOLVED_COLUMN|FIELD_NOT_FOUND|TABLE_OR_VIEW_NOT_FOUND|Cannot resolve column name|"
                      r"UNRESOLVED_FIELD|name 'df_\w+' is not defined|No such struct field|"
                      r"UNRESOLVED_ROUTINE", re.I)
SETS = [("holdout", "{m}__holdout.json"), ("holdout_v4", "{m}__holdout_v4.json"), ("test_fixed", "{m}__test_fixed.json"),
        ("complex", "{m}__complex_v8_test.json"), ("v11 held-out", "v11-heldout/{m}__heldout.json"),
        ("v12 held-out", "v12-heldout/{m}__heldout.json"), ("v13 held-out", "v13-heldout/{m}__heldout.json"),
        ("v14 held-out", "v14-heldout/{m}__heldout.json"), ("schema-shift", "schema-shift/{m}__schema_shift.json"),
        ("complex-shift", "complex-shift/{m}__complex_shift.json"),
        ("real-style", "realstyle/{m}__realstyle.json")]


def classify(path: Path):
    d = json.loads(path.read_text())
    n, fails, slips = len(d["cases"]), 0, []
    for c in d["cases"]:
        s = c["strict"]
        if s.get("match"):
            continue
        fails += 1
        if NAME_ERR.search(s.get("detail") or ""):
            slips.append(c["id"])
    return n, fails, slips


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--models", nargs="+", required=True)
    ap.add_argument("--ids", action="store_true", help="also list the ids of the name-slip cases")
    args = ap.parse_args()
    print("| Eval set | " + " | ".join(f"{m}: failures / name slips" for m in args.models) + " |")
    print("| --- |" + " --- |" * len(args.models))
    tot = {m: [0, 0] for m in args.models}
    listing = []
    for name, pat in SETS:
        cells = []
        for m in args.models:
            p = RESULTS / pat.format(m=m)
            if not p.exists():
                cells.append("—")
                continue
            n, fails, slips = classify(p)
            tot[m][0] += fails
            tot[m][1] += len(slips)
            cells.append(f"{fails} / {len(slips)} (of {n})")
            if slips:
                listing.append(f"{m} {name}: {', '.join(slips)}")
        if any(c != "—" for c in cells):
            print(f"| {name} | " + " | ".join(cells) + " |")
    print("| **all** | " + " | ".join(f"**{f} / {s}**" for f, s in tot.values()) + " |")
    if args.ids:
        print()
        print("\n".join(listing))


if __name__ == "__main__":
    main()
