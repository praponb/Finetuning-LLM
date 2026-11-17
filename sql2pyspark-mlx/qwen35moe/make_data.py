"""Qwen3.5-35B-A3B data: token lengths of the v18a mix (complex_v17/data_mix) under this model's tokenizer.

Writes  qwen35moe/data_long/{train,valid}.jsonl   (64 longest train / 8 longest valid samples, for the memory test)
        qwen35moe/data_mix<N>/{train,valid}.jsonl  (the mix without samples longer than N tokens) for N in 3200, 2600
Usage (from the repo root):  sql2pyspark-mlx/.venv/bin/python sql2pyspark-mlx/qwen35moe/make_data.py
"""
from __future__ import annotations

import json
from pathlib import Path

from transformers import AutoTokenizer

HERE = Path(__file__).resolve().parent
MIX = HERE.parent / "complex_v17/data_mix"
tok = AutoTokenizer.from_pretrained("mlx-community/Qwen3.5-35B-A3B-4bit")


def lengths(path: Path):
    rows = [json.loads(l) for l in path.open()]
    return [(len(tok.apply_chat_template(r["messages"], tokenize=True, return_dict=False)), r) for r in rows]


def write(rows, out: Path, name: str):
    out.mkdir(exist_ok=True)
    with (out / f"{name}.jsonl").open("w") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


train, valid = lengths(MIX / "train.jsonl"), lengths(MIX / "valid.jsonl")
ls = sorted(n for n, _ in train)
print(f"train {len(ls)}: median {ls[len(ls) // 2]}, p90 {ls[int(len(ls) * .9)]}, p99 {ls[int(len(ls) * .99)]}, "
      f"max {ls[-1]}, mean {sum(ls) / len(ls):.0f}, total {sum(ls):,} tokens")
for cap in (2600, 3200):
    print(f"  > {cap}: {sum(n > cap for n in ls)} train, {sum(n > cap for n, _ in valid)} valid")
    write([r for n, r in train if n <= cap], HERE / f"data_mix{cap}", "train")
    write([r for n, r in valid if n <= cap], HERE / f"data_mix{cap}", "valid")
write([r for _, r in sorted(train, key=lambda x: -x[0])[:64]], HERE / "data_long", "train")
write([r for _, r in sorted(valid, key=lambda x: -x[0])[:8]], HERE / "data_long", "valid")
print("longest 64 train:", sorted(train, key=lambda x: -x[0])[63][0], "-", sorted(train, key=lambda x: -x[0])[0][0], "tokens")
