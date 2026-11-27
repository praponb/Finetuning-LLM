"""Qwen3.5-9B run q9g data (data_q9g): one more round CONTINUED FROM q9df3 (owner 2026-11-26), no new API spend.

Train (~12k lines, shuffled):
  - nested_v20: 4,000 free template pairs (nested_v20/generate.py + verify.py, type-strict Spark verified), picked
    evenly across the 17 families, de-duplicated by SQL
  - harness_v20 pilot: the 37 Kimi pairs kept by the $10 pilot, x3
  - star_pool: 400 SELECT * / subquery pairs (keeps the q9f loop fix)
  - replay: 7,500 random lines of data_q9e/train.jsonl (keeps what q9a-q9d learned)
Valid: data_q9e/valid.jsonl (737) + 100 nested_v20 pairs not in train.
Checks: eval SQL inside train = 0 (every eval set incl. dev_short, star_probe, real_queries, real-style, harness v1/v2);
no nested_v20 / pilot pair is a near-duplicate of a harness query (token Jaccard >= 0.5); no suite table in new pairs;
valid / train overlap 0; Spark spot-check: 200 nested_v20 pairs re-verified type-strict + 200 replay pairs (build_q9e).
  .venv/bin/python qwen35_pilot/polishmix/build_q9g.py            (from sql2pyspark-mlx)
"""
from __future__ import annotations

import argparse
import json
import random
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

MLX = Path(__file__).resolve().parents[2]
P = MLX / "qwen35_pilot"
sys.path.insert(0, str(MLX))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import build_q9e as b9e  # noqa: E402

OUT = P / "data_q9g"
SEED = 2048
SYSTEM = ("You are an expert data engineer specializing in SQL and Apache Spark. Convert the provided SQL query into "
          "clean, idiomatic PySpark DataFrame code.")
SUITE_TABLES = {"customers", "products", "orders", "order_items", "payments", "fx_rates", "web_events", "promotions",
                "inventory_movements", "tier_history", "category_tree", "support_tickets", "price_history"}
lines = b9e.lines
norm = b9e.norm
sql_of = b9e.sql_of


def msg(sql: str, code: str) -> str:
    code = code.strip()
    if not code.startswith("```"):
        code = f"```python\n{code}\n```"
    return json.dumps({"messages": [{"role": "system", "content": SYSTEM},
                                    {"role": "user", "content": f"Convert SQL to PySpark:\n```sql\n{sql.strip()}\n```"},
                                    {"role": "assistant", "content": code}]}, ensure_ascii=False)


def eval_sqls() -> dict[str, list[str]]:
    E = MLX.parent / "eval"
    files = {f: E / f for f in ("holdout.jsonl", "holdout_v4.jsonl", "test_fixed.jsonl", "complex_v8_test.jsonl",
                                "dev_short.jsonl", "star_probe.jsonl", "real_queries.jsonl", "harness_suite.jsonl",
                                "harness_suite_v2.jsonl")}
    files["complex_shift"] = MLX / "complex_v16/data/complex_shift_meta.jsonl"
    files["schema_shift"] = MLX / "schema_v15/data/schema_shift_meta.jsonl"
    for v in (11, 12, 13, 14):
        files[f"v{v}_heldout"] = MLX / f"targeted_v{v}/data/heldout_meta.jsonl"
    files["realstyle_v19"] = MLX / "realstyle_v19/pilot/cases.jsonl"
    out = {}
    for k, f in files.items():
        s = []
        for x in lines(f):
            d = json.loads(x)
            q = d.get("sql") or d.get("input") or (d.get("case") or {}).get("sql") or \
                (d["messages"][1]["content"] if "messages" in d else "")
            if q:
                s.append(q)
        out[k] = s
    return out


TOK = re.compile(r"[A-Za-z_][A-Za-z0-9_]*|\d+|[^\sA-Za-z0-9_]")


def toks(s: str) -> set[str]:
    return {t.lower() for t in TOK.findall(s)}


def jaccard(a: set, b: set) -> float:
    return len(a & b) / max(1, len(a | b))


def respot_nested(recs: list[dict], n: int, rng: random.Random) -> dict:
    sys.path.insert(0, str(MLX / "nested_v20"))
    import verify as nv
    nv._init()
    st = Counter()
    for r in rng.sample(recs, min(n, len(recs))):
        st[nv.check(r)["status"]] += 1
    return dict(st)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--nested", default=str(MLX / "nested_v20/raw/verified.jsonl"))
    ap.add_argument("--n-nested", type=int, default=4000)
    ap.add_argument("--n-replay", type=int, default=7500)
    ap.add_argument("--spot", type=int, default=200)
    a = ap.parse_args()
    rng = random.Random(SEED)
    ev = eval_sqls()
    ev_norm = {norm(s) for v in ev.values() for s in v}
    ev_big = [e for e in ev_norm if len(e) > 40]
    harness_toks = [toks(s) for k in ("harness_suite.jsonl", "harness_suite_v2.jsonl") for s in ev[k]]

    # nested_v20: verified ok, no suite table, not in / near an eval query, de-duplicated by SQL; even across families
    drop = Counter()
    by_fam, seen = defaultdict(list), set()
    for x in lines(a.nested):
        r = json.loads(x)
        if r.get("status") != "ok":
            drop["nested_not_ok"] += 1
            continue
        if set(r["tables"]) & SUITE_TABLES:
            drop["nested_suite_table"] += 1
            continue
        k = norm(r["sql"])
        if k in seen:
            drop["nested_dup_sql"] += 1
            continue
        if k in ev_norm or any(e in k for e in ev_big):
            drop["nested_eval_sql"] += 1
            continue
        t = toks(r["sql"])
        if any(jaccard(t, h) >= 0.5 for h in harness_toks):
            drop["nested_near_harness"] += 1
            continue
        seen.add(k)
        by_fam[r["family"]].append(r)
    for v in by_fam.values():
        rng.shuffle(v)
    want = a.n_nested + 100
    picked, fams = [], sorted(by_fam)
    while len(picked) < want and any(by_fam[f] for f in fams):   # round robin = as even as the pools allow
        for f in fams:
            if by_fam[f] and len(picked) < want:
                picked.append(by_fam[f].pop())
    rng.shuffle(picked)
    nested_valid, nested_train = picked[:100], picked[100:]

    # harness_v20 pilot (Kimi, type-strict verified) x3
    pilot = []
    for x in lines(MLX / "harness_v20/pilot/cases.jsonl"):
        r = json.loads(x)
        if not r.get("ok"):
            continue
        c = r["case"]
        if set(c["tables"]) & SUITE_TABLES or any(jaccard(toks(c["sql"]), h) >= 0.5 for h in harness_toks):
            drop["pilot_suite_or_near"] += 1
            continue
        pilot.append(msg(c["sql"], c["code"]))

    star = [msg(json.loads(x)["sql"], json.loads(x)["reference"]) for x in lines(P / "selfplay/star_pool.jsonl")]
    q9e = lines(P / "data_q9e/train.jsonl")
    replay = rng.sample(q9e, a.n_replay)

    parts = {"nested_v20": [msg(r["sql"], r["code"]) for r in nested_train], "harness_v20_pilot_x3": pilot * 3,
             "star_pool": star, "replay_q9e": replay}
    train = [x for v in parts.values() for x in v]
    rng.shuffle(train)
    valid = lines(P / "data_q9e/valid.jsonl") + [msg(r["sql"], r["code"]) for r in nested_valid]

    tr_norm = {norm(sql_of(x)) for x in train}
    eval_in_train = sum(1 for t in tr_norm if t in ev_norm or any(e in t for e in ev_big))
    overlap = sum(1 for x in valid if norm(sql_of(x)) in tr_norm)
    chars = sorted(len(x) for x in train)
    fam_counts = Counter(r["family"] for r in nested_train)
    print(f"eval SQL strings: {len(ev_norm)}; inside train: {eval_in_train}; valid/train overlap: {overlap}")

    OUT.mkdir(exist_ok=True)
    (OUT / "train.jsonl").write_text("\n".join(train) + "\n")
    (OUT / "valid.jsonl").write_text("\n".join(valid) + "\n")
    stats = {k: len(v) for k, v in parts.items()} | {
        "train": len(train), "valid": len(valid), "nested_families": dict(sorted(fam_counts.items())),
        "dropped": dict(drop), "eval_sql_inside_train": eval_in_train, "valid_train_overlap": overlap,
        "max_line_chars": chars[-1], "mean_line_chars": round(sum(chars) / len(chars))}
    if a.spot:
        b9e.OUT = OUT
        stats["spot_check_nested_v20"] = respot_nested(nested_train, a.spot, rng)
        stats["spot_check_replay"] = b9e.spot_check(replay + star, a.spot, rng)
    (OUT / "stats.json").write_text(json.dumps(stats, indent=2) + "\n")
    print(json.dumps(stats, indent=2))
    if eval_in_train or overlap:
        raise SystemExit("LEAKAGE / OVERLAP — do not train on this mix")


if __name__ == "__main__":
    main()
