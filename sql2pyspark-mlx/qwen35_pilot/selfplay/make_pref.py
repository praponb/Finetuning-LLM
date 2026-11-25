"""Step C -> D (11-23 improvement plan): preference pairs from collect.py's answers.

For every pool query with at least one WRONG answer:
  rejected = that wrong answer (up to 2 distinct per query, the greedy one first: it is what the evals see)
  chosen   = one of the model's OWN correct answers to the same query if there is one (on-policy, same style),
             otherwise the pool's reference answer (Checker-verified when the pool was built)
Failure types are counted for the report: name (unknown df_ / unresolved column), py_error, rows, columns, syntax,
no_result, loop (reply cut at the token limit / repeated lines). 90 / 10 split by query.
  .venv/bin/python qwen35_pilot/selfplay/make_pref.py --raw qwen35_pilot/selfplay/raw_q9e-q4.jsonl
-> qwen35_pilot/data_q9f/pref_train.jsonl, pref_valid.jsonl, pref_stats.json
   line: {"id", "messages": [system, user], "chosen": str, "rejected": str, "chosen_from": own|reference, "type"}
"""
from __future__ import annotations

import argparse
import json
import random
import re
from collections import Counter, defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = HERE.parent / "data_q9f"


def ftype(r: dict) -> str:
    d, rep = r["detail"], r["reply"]
    lines = [x for x in rep.splitlines() if x.strip()]
    if "```python" not in rep or (len(lines) > 30 and len(set(lines)) < len(lines) * 0.5):
        return "loop"
    if re.search(r"NameError: name 'df_|UNRESOLVED_COLUMN|cannot be resolved", d):
        return "name"
    return r["status"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", required=True)
    ap.add_argument("--pool", default=str(HERE / "pool.jsonl"))
    ap.add_argument("--out", default=str(OUT), help="output dir (default data_q9f)")
    ap.add_argument("--max-rejected", type=int, default=2)
    a = ap.parse_args()
    out_dir = Path(a.out)
    pool = {d["id"]: d for d in map(json.loads, Path(a.pool).read_text().splitlines())}
    by_id = defaultdict(list)
    for x in Path(a.raw).read_text().splitlines():
        if x.strip():
            r = json.loads(x)
            if not r["reply"].startswith("__ERROR__"):
                by_id[r["id"]].append(r)
    stats, rows = Counter(), []
    for qid, rs in by_id.items():
        rs.sort(key=lambda r: r["seed"])
        bad = [r for r in rs if not r["ok"]]
        good = [r for r in rs if r["ok"]]
        stats["queries"] += 1
        stats["greedy_fail"] += any(r["seed"] == 0 and not r["ok"] for r in rs)
        if not bad:
            continue
        p = pool[qid]
        chosen, src = (good[0]["reply"], "own") if good else (p["reference"], "reference")
        seen = set()
        for r in bad:
            if r["reply"] in seen or len(seen) >= a.max_rejected:
                continue
            seen.add(r["reply"])
            t = ftype(r)
            stats[f"type_{t}"] += 1
            stats[f"chosen_{src}"] += 1
            rows.append({"id": qid, "messages": [{"role": "system", "content": p["system"]},
                                                 {"role": "user", "content": f"Convert SQL to PySpark:\n```sql\n{p['sql']}\n```"}],
                         "chosen": chosen, "rejected": r["reply"], "chosen_from": src, "type": t,
                         "greedy": r["seed"] == 0})
    ids = sorted({r["id"] for r in rows})
    random.Random(2036).shuffle(ids)
    val_ids = set(ids[:max(1, len(ids) // 10)])
    out_dir.mkdir(exist_ok=True)
    tr = [r for r in rows if r["id"] not in val_ids]
    va = [r for r in rows if r["id"] in val_ids]
    (out_dir / "pref_train.jsonl").write_text("".join(json.dumps(r) + "\n" for r in tr))
    (out_dir / "pref_valid.jsonl").write_text("".join(json.dumps(r) + "\n" for r in va))
    stats.update({"pairs": len(rows), "pairs_train": len(tr), "pairs_valid": len(va), "queries_with_pairs": len(ids)})
    (out_dir / "pref_stats.json").write_text(json.dumps(dict(stats), indent=2) + "\n")
    print(json.dumps(dict(stats), indent=2))


if __name__ == "__main__":
    main()
