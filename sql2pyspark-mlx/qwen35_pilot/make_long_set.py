"""Pilot step 2 data: token lengths of the v15 mix under the Qwen3.5 tokenizer, and the 64 longest samples.

Writes qwen35_pilot/data_long/{train,valid}.jsonl (64 longest train samples; valid = the 8 longest valid samples)
for the memory test, and prints how many samples each max_seq_length would truncate.

Usage (from the repo root):  sql2pyspark-mlx/.venv/bin/python sql2pyspark-mlx/qwen35_pilot/make_long_set.py
"""
from __future__ import annotations

import json
from pathlib import Path

from transformers import AutoTokenizer

HERE = Path(__file__).resolve().parent
MIX = HERE.parent / "schema_v15/data_mix"
OUT = HERE / "data_long"

tok = AutoTokenizer.from_pretrained("mlx-community/Qwen3.5-9B-4bit")


def lengths(path: Path):
    rows = [json.loads(l) for l in path.open()]
    return [(len(tok.apply_chat_template(r["messages"], tokenize=True, return_dict=False)), r) for r in rows]


train = lengths(MIX / "train.jsonl")
valid = lengths(MIX / "valid.jsonl")
ls = sorted(n for n, _ in train)
print(f"train {len(ls)} samples: median {ls[len(ls) // 2]}, p99 {ls[int(len(ls) * .99)]}, max {ls[-1]}")
for cut in (2000, 2600, 3200, 4000):
    k = sum(n > cut for n in ls)
    print(f"  > {cut}: {k} ({100 * k / len(ls):.1f}%)")

OUT.mkdir(exist_ok=True)
for name, rows, k in (("train", train, 64), ("valid", valid, 8)):
    top = sorted(rows, key=lambda x: -x[0])[:k]
    with (OUT / f"{name}.jsonl").open("w") as f:
        for _, r in top:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"{name}: {k} longest, {top[-1][0]}..{top[0][0]} tokens")
