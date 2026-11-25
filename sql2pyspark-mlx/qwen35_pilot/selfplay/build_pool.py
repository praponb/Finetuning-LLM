"""Step C pool (11-23 improvement plan): SQL queries the model has NOT been trained on, for collecting its own mistakes.

Sources: data/train.jsonl (v1-era generator pairs) and targeted_v11..v14/data/train.jsonl, keeping only pairs
  - whose SQL is not in data_q9e (the q9e mix, which contains the v18a mix) and not seen earlier in this pool,
  - whose tables verify_pairs.Checker knows (df_<schemas.py table>),
  - whose SQL (normalized, > 40 chars) does not contain / equal any eval query or dev_short query (leakage 0),
  - whose reference code MATCHES the SQL in the Checker (so it can serve as a "chosen" answer).
N = 6,000 (default): half short (SQL < 400 chars, hold-out-like), half any length; 40% data/train, 60% targeted.
  .venv/bin/python qwen35_pilot/selfplay/build_pool.py [--n 6000]      (from sql2pyspark-mlx)
-> qwen35_pilot/selfplay/pool.jsonl  {id, source, sql, reference (assistant content), messages_system}, pool_stats.json
"""
from __future__ import annotations

import argparse
import json
import random
import re
import sys
from collections import Counter
from pathlib import Path

MLX = Path(__file__).resolve().parents[2]
ROOT = MLX.parent
sys.path.insert(0, str(MLX))
OUT = Path(__file__).resolve().parent
norm = lambda s: " ".join(s.split()).lower().rstrip(";").strip()
SQL_RE = re.compile(r"```sql\n(.*?)```", re.S)
CODE_RE = re.compile(r"```python\n(.*?)```", re.S)


def eval_sql() -> set[str]:
    evs = [ROOT / "eval" / f for f in ("holdout.jsonl", "holdout_v4.jsonl", "test_fixed.jsonl", "complex_v8_test.jsonl",
                                        "real_queries.jsonl", "harness_suite.jsonl", "dev_short.jsonl")]
    evs += [MLX / "complex_v16/data/complex_shift_meta.jsonl", MLX / "schema_v15/data/schema_shift_meta.jsonl",
            MLX / "classic_v10/data/inlist_valid_meta.jsonl"]
    evs += [MLX / f"targeted_v{v}/data/heldout_meta.jsonl" for v in (11, 12, 13, 14)]
    out = set()
    for f in evs:
        for x in f.read_text().splitlines():
            if x.strip():
                d = json.loads(x)
                s = d.get("sql") or d.get("input") or (d["messages"][1]["content"] if "messages" in d else "")
                m = SQL_RE.search(s)
                out.add(norm(m.group(1) if m else s))
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=6000)
    a = ap.parse_args()
    rng = random.Random(2034)
    from schemas import EXTRA_TABLES, TABLES
    known = {f"df_{t.name}" for t in TABLES + EXTRA_TABLES}
    q9e = {json.loads(x)["messages"][1]["content"] for x in (MLX / "qwen35_pilot/data_q9e/train.jsonl").read_text()
           .splitlines() if x.strip()}
    evs = eval_sql()
    big = [e for e in evs if len(e) > 40]
    stats = Counter()
    cands: dict[str, list[dict]] = {"data": [], "targeted": []}
    seen = set()
    files = [("data", MLX / "data/train.jsonl")] + [("targeted", MLX / f"targeted_v{v}/data/train.jsonl")
                                                    for v in (11, 12, 13, 14)]
    for group, f in files:
        for x in f.read_text().splitlines():
            if not x.strip():
                continue
            m = json.loads(x)["messages"]
            user, ref = m[1]["content"], m[2]["content"]
            if user in q9e or user in seen:
                stats["skip_in_q9e_or_dup"] += 1
                continue
            seen.add(user)
            tabs = set(re.findall(r"\bdf_(\w+)", ref))
            if not tabs or not all(f"df_{t}" in known for t in tabs):
                stats["skip_unknown_tables"] += 1
                continue
            sm = SQL_RE.search(user)
            if not sm or not CODE_RE.search(ref):
                stats["skip_format"] += 1
                continue
            sql = sm.group(1).strip()
            ns = norm(sql)
            if ns in evs or any(e in ns for e in big):
                stats["skip_eval_leak"] += 1
                continue
            cands[group].append({"source": f"{f.parent.parent.name}/{f.name}", "sql": sql, "reference": ref,
                                 "system": m[0]["content"]})
    # sample more than needed, then keep the ones whose reference matches in Spark
    want = {"data": int(a.n * 0.4), "targeted": a.n - int(a.n * 0.4)}
    from verify_pairs import Checker
    ck = Checker()
    pool = []
    for group, n in want.items():
        c = cands[group]
        rng.shuffle(c)
        short = [x for x in c if len(x["sql"]) < 400]
        picked, tried, n_short = [], set(), n // 2
        for label, src, k in (("short", short, n_short), ("any", c, n - n_short)):
            got = 0
            for x in src:
                if got >= k:
                    break
                if id(x) in tried:
                    continue
                tried.add(id(x))
                r = ck.check(x["sql"], CODE_RE.search(x["reference"]).group(1))
                stats[f"ref_check_{'ok' if r.ok else 'fail'}"] += 1
                if r.ok:
                    picked.append(x)
                    got += 1
            stats[f"{group}_{label}"] += got
        pool += picked
    rng.shuffle(pool)
    for i, x in enumerate(pool):
        x["id"] = f"sp{i:05d}"
    (OUT / "pool.jsonl").write_text("".join(json.dumps(x) + "\n" for x in pool))
    stats.update({"pool": len(pool), "candidates_data": len(cands["data"]),
                  "candidates_targeted": len(cands["targeted"]), "eval_strings": len(evs),
                  "short_lt_400": sum(len(x["sql"]) < 400 for x in pool)})
    (OUT / "pool_stats.json").write_text(json.dumps(dict(stats), indent=2) + "\n")
    print(json.dumps(dict(stats), indent=2))


if __name__ == "__main__":
    main()
