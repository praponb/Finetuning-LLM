"""Build the complex_v9 training data: the complex_v8 pairs with "mirror" window code + extra window-heavy pairs.

The SQL side and the train / valid / test split are those of complex_v8 (same generator seed, so every query is
identical; only the PySpark window style changed), which keeps eval/complex_v8_test.jsonl held out and the v8 scores
comparable.

  data/      all re-rendered complex_v8 pairs (same split) + the extra window-heavy pairs (train only)
  data_mix/  what v9 trains on: every pair that uses a window (the code that changed) + --other complex pairs
             without windows + --mix-old classic pairs from ../data/ (v7 training set)

Rules: only pairs verified in Spark (raw_pairs/verified.jsonl, raw_extra/verified.jsonl); pairs longer than
--max-tokens are dropped; extra pairs must have a non-empty result, a feature signature that is not held out in
complex_v8 valid/test, and SQL that is in no eval set.

Usage:  python build.py [--other 2000 --mix-old 4000]
"""
from __future__ import annotations

import argparse
import json
import random
import sys
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
V8 = HERE.parent / "complex_v8"
sys.path.insert(0, str(HERE.parent))

from convert_sql_pyspark import DEFAULT_SYSTEM_PROMPT, format_sample, norm_sql, signature, write_jsonl  # noqa: E402

EVAL_SETS = ["holdout.jsonl", "holdout_v4.jsonl", "real_queries.jsonl", "test_fixed.jsonl", "complex_v8_test.jsonl"]
BASE = "mlx-community/Qwen2.5-Coder-7B-Instruct-4bit"
META_KEYS = ("id", "family", "features", "order_cols", "sql", "code")


def read_jsonl(p: Path) -> list:
    return [json.loads(line) for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]


def chat_sql(row: dict) -> str:
    """SQL of a chat-format training row (inverse of format_sample)."""
    text = row["messages"][1]["content"]
    return norm_sql(text.split("```sql\n", 1)[1].rsplit("\n```", 1)[0])


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--max-tokens", type=int, default=3000)
    ap.add_argument("--other", type=int, default=2000, help="complex pairs without windows kept in data_mix/")
    ap.add_argument("--mix-old", type=int, default=4000, help="v7 training rows added to data_mix/")
    ap.add_argument("--seed", type=int, default=9)
    args = ap.parse_args()
    rng = random.Random(args.seed)

    from transformers import AutoTokenizer
    tok = AutoTokenizer.from_pretrained(BASE)

    def n_tokens(r) -> int:
        text = tok.apply_chat_template(format_sample(r["sql"], r["code"], DEFAULT_SYSTEM_PROMPT)["messages"],
                                       tokenize=False)
        return len(tok.encode(text))

    v8_sql = {n: {chat_sql(r) for r in read_jsonl(V8 / "data" / f"{n}.jsonl")} for n in ("train", "valid", "test")}
    held_sigs = {signature(r) for n in ("valid", "test") for r in read_jsonl(V8 / "data" / f"{n}_meta.jsonl")}
    excluded = set(v8_sql["valid"]) | set(v8_sql["test"])
    for name in EVAL_SETS:
        excluded |= {norm_sql(r["sql"]) for r in read_jsonl(HERE.parent.parent / "eval" / name)}

    recs = read_jsonl(HERE / "raw_pairs" / "verified.jsonl")
    for r in recs:
        r["n_tokens"] = n_tokens(r)
    parts = {n: [r for r in recs if norm_sql(r["sql"]) in v8_sql[n] and r["n_tokens"] <= args.max_tokens]
             for n in ("train", "valid", "test")}
    lost = {n: len(v8_sql[n]) - len(parts[n]) for n in parts}       # not verified in the new style or too long

    extra_all = read_jsonl(HERE / "raw_extra" / "verified.jsonl")
    extra = []
    for r in extra_all:
        if r["n_rows"] == 0 or signature(r) in held_sigs or norm_sql(r["sql"]) in excluded:
            continue
        r["n_tokens"] = n_tokens(r)
        if r["n_tokens"] <= args.max_tokens:
            extra.append(r)

    train = parts["train"] + extra
    rng.shuffle(train)
    out = HERE / "data"
    out.mkdir(exist_ok=True)
    for name, rows in (("train", train), ("valid", parts["valid"]), ("test", parts["test"])):
        write_jsonl(out / f"{name}.jsonl", [format_sample(r["sql"], r["code"], DEFAULT_SYSTEM_PROMPT) for r in rows])
    for name in ("valid", "test"):
        write_jsonl(out / f"{name}_meta.jsonl", [{k: r[k] for k in META_KEYS} for r in parts[name]])

    windowed = [r for r in train if "Window." in r["code"]]
    plain = [r for r in train if "Window." not in r["code"]]
    mixed_recs = windowed + rng.sample(plain, min(args.other, len(plain)))
    mixed = [json.dumps(format_sample(r["sql"], r["code"], DEFAULT_SYSTEM_PROMPT), ensure_ascii=False) for r in mixed_recs]
    old_dir = HERE.parent / "data"
    old_train = [line for line in (old_dir / "train.jsonl").read_text().splitlines() if line.strip()]
    old_valid = [line for line in (old_dir / "valid.jsonl").read_text().splitlines() if line.strip()]
    n_new = len(mixed)
    mixed += rng.sample(old_train, min(args.mix_old, len(old_train)))
    rng.shuffle(mixed)
    new_valid = (out / "valid.jsonl").read_text().splitlines()
    mvalid = new_valid + rng.sample(old_valid, min(len(new_valid), len(old_valid)))
    rng.shuffle(mvalid)
    mix = HERE / "data_mix"
    mix.mkdir(exist_ok=True)
    (mix / "train.jsonl").write_text("\n".join(mixed) + "\n")
    (mix / "valid.jsonl").write_text("\n".join(mvalid) + "\n")
    (mix / "test.jsonl").write_text((out / "test.jsonl").read_text())

    toks = sorted(r["n_tokens"] for r in mixed_recs)
    stats = {
        "verified_pairs": len(recs), "verified_extra": len(extra_all), "extra_used": len(extra),
        "v8_pairs_lost": lost, "train": len(train), "valid": len(parts["valid"]), "test": len(parts["test"]),
        "train_with_windows": len(windowed), "train_without_windows": len(plain),
        "signature_overlap_train_heldout": len({signature(r) for r in extra} & held_sigs),
        "family_counts_train": dict(Counter(r["family"] for r in train)),
        "data_mix": {"train": len(mixed), "complex": n_new, "from_v7": len(mixed) - n_new, "valid": len(mvalid),
                     "complex_tokens": {"mean": round(sum(toks) / len(toks)), "p50": toks[len(toks) // 2],
                                        "p95": toks[int(len(toks) * .95)], "max": toks[-1]}},
    }
    (out / "split_stats.json").write_text(json.dumps(stats, indent=2))
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
