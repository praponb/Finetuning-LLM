"""Build the v17 training data: v15's full mix (schema_v15/data_mix, 25,008 pairs) plus a smaller share of v16's
complex pairs on the synthetic catalog (complex_v16/data/train.jsonl, which already excludes the complex
schema-shift set). v16 trained on 10,000 complex of 21,008 (48%): it won the complex schema-shift set but lost on the
earlier forms; v17 keeps v15's replay whole and adds --complex pairs (default 4,000, ~14%).

  python build.py     -> data_mix/ (train = v15 mix + complex sample, shuffled; valid = v16's valid set, unchanged)
"""
from __future__ import annotations

import argparse
import json
import random
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
MLX = HERE.parent


def lines_of(p: Path) -> list:
    return [x for x in p.read_text(encoding="utf-8").splitlines() if x.strip()]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--complex", type=int, default=4000)
    ap.add_argument("--seed", type=int, default=17)
    args = ap.parse_args()
    rng = random.Random(args.seed)

    v15 = lines_of(MLX / "schema_v15" / "data_mix" / "train.jsonl")
    cx = lines_of(MLX / "complex_v16" / "data" / "train.jsonl")
    pick = rng.sample(cx, args.complex)
    lines = v15 + pick
    rng.shuffle(lines)
    valid = lines_of(MLX / "complex_v16" / "data_mix" / "valid.jsonl")
    sql = lambda x: json.loads(x)["messages"][1]["content"]  # noqa: E731
    seen_valid = {sql(x) for x in valid}
    leaks = sum(sql(x) in seen_valid for x in lines)

    out = HERE / "data_mix"
    out.mkdir(exist_ok=True)
    (out / "train.jsonl").write_text("\n".join(lines) + "\n")
    (out / "valid.jsonl").write_text("\n".join(valid) + "\n")
    chars = [len(sql(x)) + len(json.loads(x)["messages"][2]["content"]) for x in lines]
    stats = {"train": len(lines), "schema_v15_mix": len(v15), "complex_v16": len(pick),
             "complex_share": round(len(pick) / len(lines), 3), "valid": len(valid), "train_valid_overlap": leaks,
             "mean_chars": round(sum(chars) / len(chars)), "max_chars": max(chars)}
    (out / "stats.json").write_text(json.dumps(stats, indent=2) + "\n")
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
