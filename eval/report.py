"""Build a markdown comparison table from eval/results/*.json and (optionally) put it in README.md.

Usage:
  python eval/report.py                      # print the table
  python eval/report.py --readme README.md   # also replace the block between the RESULTS markers
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
START, END = "<!-- RESULTS:START -->", "<!-- RESULTS:END -->"


SET_ORDER = ["holdout", "holdout_v4", "real_queries", "test_meta"]


def load():
    runs = []
    for f in sorted((ROOT / "eval" / "results").glob("*.json")):
        d = json.loads(f.read_text())
        runs.append((d["model"], Path(d["set"]).stem, d))
    # group rows by eval set, then by model
    return sorted(runs, key=lambda r: (SET_ORDER.index(r[1]) if r[1] in SET_ORDER else len(SET_ORDER), r[1], r[0]))


def table(runs) -> str:
    lines = ["| Model | Eval set | n | Syntax OK | Runs (strict) | Correct (strict) | Runs (lenient) | Correct (lenient) | s / query |",
             "|---|---|---|---|---|---|---|---|---|"]
    for model, set_name, d in runs:
        a = d["summary"]["ALL"]
        lines.append(f"| `{model}` | {set_name} | {a['n']} | {a['strict_syntax']:.0%} | {a['strict_runs']:.0%} | "
                     f"**{a['strict_match']:.0%}** | {a['lenient_runs']:.0%} | **{a['lenient_match']:.0%}** | "
                     f"{d.get('avg_seconds', 0):.1f} |")
    hold = [(m, d) for m, s, d in runs if s == "holdout"]
    if hold:
        levels = [k for k in ("simple", "medium", "complex") if k in hold[0][1]["summary"]]
        lines += ["", "Hold-out, correct results (lenient) by difficulty:", "",
                  "| Model | " + " | ".join(f"{lv} (n={hold[0][1]['summary'][lv]['n']})" for lv in levels) + " |",
                  "|---|" + "---|" * len(levels)]
        for m, d in hold:
            lines.append(f"| `{m}` | " + " | ".join(f"{d['summary'][lv]['lenient_match']:.0%}" for lv in levels) + " |")
    v4 = [(m, d) for m, s, d in runs if s == "holdout_v4"]
    if v4:
        groups = [k for k in ("aggregation", "window", "join", "setop", "reshape", "nested", "pipeline")
                  if k in v4[0][1]["summary"]]
        lines += ["", "v4 hold-out, correct results (lenient) by feature group:", "",
                  "| Model | " + " | ".join(f"{g} (n={v4[0][1]['summary'][g]['n']})" for g in groups) + " |",
                  "|---|" + "---|" * len(groups)]
        for m, d in v4:
            lines.append(f"| `{m}` | " + " | ".join(f"{d['summary'][g]['lenient_match']:.0%}" for g in groups) + " |")
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--readme", default=None)
    args = ap.parse_args()
    md = table(load())
    print(md)
    if args.readme:
        p = Path(args.readme)
        s = p.read_text()
        block = f"{START}\n{md}\n{END}"
        if START in s:
            s = re.sub(re.escape(START) + r".*?" + re.escape(END), lambda _: block, s, flags=re.S)
        else:
            s = s.replace("RESULTS_PLACEHOLDER", block)
        p.write_text(s)
        print(f"\nupdated {p}")


if __name__ == "__main__":
    main()
