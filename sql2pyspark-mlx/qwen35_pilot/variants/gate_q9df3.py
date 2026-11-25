"""Quick gate for the q9df3 variants (option 1, 11-25): star probe matches + loops, hold-out / v4 / fixed / dev.
A loop = a non-empty line repeated >= 6 times, or a reply that never closes its code block (cut at num_predict).
  python3 gate_q9df3.py tag ...           -> table
  python3 gate_q9df3.py --best tag ...    -> prints the best tag passing (0 loops, hold-out >= 61, v4 >= 20, fixed >= 145), or NONE
"""
import json, os, sys
from collections import Counter
R = "eval/results/variants"

def load(m, s):
    for p in (f"{R}/{m}__{s}.json", f"eval/results/{m}__{s}.json"):
        if os.path.exists(p):
            return json.load(open(p))["cases"]
    return None

def is_loop(reply):
    lines = Counter(l.strip() for l in reply.splitlines() if len(l.strip()) > 3 and not l.strip().startswith("```"))
    return (lines and max(lines.values()) >= 6) or reply.count("```") % 2 == 1

def row(m):
    sp = load(m, "star_probe")
    r = {"model": m, "star": None, "loops": None}
    if sp:
        r["star"] = sum(c["strict"]["match"] for c in sp); r["loops"] = sum(bool(is_loop(c["reply"])) for c in sp)
    for s in ("holdout", "holdout_v4", "test_fixed", "dev_short"):
        c = load(m, s); r[s] = None if c is None else sum(x["strict"]["match"] for x in c)
    return r

def passes(r):
    return r["loops"] == 0 and (r["holdout"] or 0) >= 61 and (r["holdout_v4"] or 0) >= 20 and (r["test_fixed"] or 0) >= 145

if __name__ == "__main__":
    a = sys.argv[1:]
    best = a and a[0] in ("--best", "--all")
    rows = [row(m) for m in (a[1:] if best else a)]
    if a and a[0] == "--all":   # all passing tags, best first
        ok = sorted([r for r in rows if passes(r)], key=lambda r: (r["holdout"], r["holdout"] + r["holdout_v4"] + r["test_fixed"] + (r["dev_short"] or 0)), reverse=True)
        print(" ".join(r["model"] for r in ok))
    elif best:
        ok = [r for r in rows if passes(r)]
        print(max(ok, key=lambda r: (r["holdout"], r["holdout"] + r["holdout_v4"] + r["test_fixed"] + (r["dev_short"] or 0)))["model"] if ok else "NONE")
    else:
        print("| Model | Star probe | Loops | Hold-out | v4 | Fixed | Dev | Gate |\n|---|---|---|---|---|---|---|---|")
        for r in rows:
            print(f"| {r['model']} | {r['star']} | {r['loops']} | {r['holdout']} | {r['holdout_v4']} | {r['test_fixed']} | {r['dev_short']} | {'PASS' if passes(r) else 'no'} |")
