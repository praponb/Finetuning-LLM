"""Quick gate for the q9g candidates (2026-11-26): gate_q9df3.py's star probe / hold-out / v4 / fixed / dev + harness v2
(type-strict PASS count of eval/results/harness/<tag>__harness_suite_v2.strict.json, baseline q9df3-q4 6/50).
  python3 gate_q9g.py tag ...            -> table
  python3 gate_q9g.py --all tag ...      -> tags passing the gate (0 loops, hold-out >= 61, v4 >= 20, fixed >= 145),
                                            best first (harness v2 + hold-out + v4 + fixed + dev)
  python3 gate_q9g.py --harness tag      -> harness v2 strict PASS count (or -1)
"""
import json, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from gate_q9df3 import row as row0, passes

def hv2(m):
    p = f"eval/results/harness/{m}__harness_suite_v2.strict.json"
    return json.load(open(p))["counts"].get("PASS", 0) if os.path.exists(p) else None

def row(m):
    r = row0(m); r["harness_v2"] = hv2(m); return r

def key(r):
    return (r["harness_v2"] or 0) + r["holdout"] + r["holdout_v4"] + r["test_fixed"] + (r["dev_short"] or 0)

if __name__ == "__main__":
    a = sys.argv[1:]
    if a and a[0] == "--harness":
        v = hv2(a[1]); print(-1 if v is None else v)
    elif a and a[0] == "--all":
        print(" ".join(r["model"] for r in sorted([r for r in map(row, a[1:]) if passes(r)], key=key, reverse=True)))
    else:
        print("| Model | Star probe | Loops | Hold-out | v4 | Fixed | Dev | Harness v2 | Gate |\n|---|---|---|---|---|---|---|---|---|")
        for r in map(row, a):
            print(f"| {r['model']} | {r['star']} | {r['loops']} | {r['holdout']} | {r['holdout_v4']} | {r['test_fixed']} | "
                  f"{r['dev_short']} | {r['harness_v2']} | {'PASS' if passes(r) else 'no'} |")
