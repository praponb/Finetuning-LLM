"""Qwen3.5-9B run 9 data (data_m6h): a complex-focused polish set for the run-8 adapter.

Run 8 (q35m6g-q4) tied v15-q4 except on the complex set (82 vs 89/100). The v17 mix that runs 6-8 trained on holds only
3,500 of complex_v9's 11,989 pairs (the complex_v8 catalog, "mirror" window code), so most complex pairs are unseen.
Train (shuffled, seed 2028):
  5,000 complex_v9 pairs NOT in the v17 mix        (complex_v8 eval catalog; train split excludes the test signatures)
  2,000 complex_v16 pairs NOT in the v17 mix       (synthetic catalog; keeps the complex-shift gains)
  2,000 renamed pairs from data_m6g                (name copying, run 8)
  3,000 other v17-mix pairs (no complex_v9/v16)    (replay for hold-out / fixed / v4)
Valid: data_m6g/valid.jsonl (same as run 8).
  python qwen35_pilot/complexmix/build.py   (from sql2pyspark-mlx)
"""
import json, random
from collections import Counter
from pathlib import Path

MLX = Path(__file__).resolve().parents[2]
P = MLX / "qwen35_pilot"
OUT = P / "data_m6h"
lines = lambda p: [x for x in p.read_text(encoding="utf-8").splitlines() if x.strip()]
sql = lambda x: json.loads(x)["messages"][1]["content"]

rng = random.Random(2028)
mix = lines(MLX / "complex_v17" / "data_mix" / "train.jsonl")
mix_sql = {sql(x) for x in mix}
v9 = lines(MLX / "complex_v9" / "data" / "train.jsonl")
v16 = lines(MLX / "complex_v16" / "data" / "train.jsonl")
cx_sql = {sql(x) for x in v9 + v16} | {sql(x) for x in lines(MLX / "complex_v8" / "data" / "train.jsonl")}
v9_new = [x for x in v9 if sql(x) not in mix_sql]
v16_new = [x for x in v16 if sql(x) not in mix_sql]
renamed = [x for x in lines(P / "data_m6g" / "train.jsonl") if sql(x) not in mix_sql]
replay = [x for x in mix if sql(x) not in cx_sql]
print(f"pools: complex_v9 new {len(v9_new)}, complex_v16 new {len(v16_new)}, renamed {len(renamed)}, replay {len(replay)}")

parts = {"complex_v9": rng.sample(v9_new, 5000), "complex_v16": rng.sample(v16_new, 2000),
         "renamed": rng.sample(renamed, 2000), "replay": rng.sample(replay, 3000)}
train = [x for v in parts.values() for x in v]
rng.shuffle(train)
valid = lines(P / "data_m6g" / "valid.jsonl")

# leakage checks against every eval set used by the chain
evs = [MLX.parent / "eval" / f for f in ("holdout.jsonl", "holdout_v4.jsonl", "test_fixed.jsonl", "complex_v8_test.jsonl")]
evs.append(MLX / "complex_v16" / "data" / "complex_shift_meta.jsonl")
ev_sql = set()
for f in evs:
    for x in lines(f):
        d = json.loads(x)
        s = d.get("sql") or d.get("input") or (d["messages"][1]["content"] if "messages" in d else "")
        ev_sql.add(" ".join(s.split()).lower())
norm_train = {" ".join(sql(x).split()).lower() for x in train}
print(f"eval SQL strings checked: {len(ev_sql)}")
hits = sum(1 for t in norm_train if any(len(e) > 40 and e in t for e in ev_sql))
OUT.mkdir(exist_ok=True)
(OUT / "train.jsonl").write_text("\n".join(train) + "\n")
(OUT / "valid.jsonl").write_text("\n".join(valid) + "\n")
stats = {k: len(v) for k, v in parts.items()} | {"train": len(train), "valid": len(valid), "eval_sql_inside_train": hits}
(OUT / "stats.json").write_text(json.dumps(stats, indent=2) + "\n")
print(json.dumps(stats, indent=2))
