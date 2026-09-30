"""Turn verified complex_v8 pairs into ready-to-train MLX chat JSONL.

  data/      exactly --train (default 10,000) training pairs + valid + test, all new complex pairs
  data_mix/  the same new pairs + a random sample of the current v7 training set (../data/), so a model
             continued from v7 keeps its classic-query accuracy (v4 lost 12 classic queries when classic
             samples fell to ~58% of the mix)

Rules: only pairs whose SQL and PySpark results matched in Spark (raw_pairs/verified.jsonl); pairs with a
non-empty result are preferred; pairs longer than --max-tokens (Qwen2.5 chat template) are dropped; SQL that
appears in any eval set is excluded; the split holds out whole feature signatures (reused from
../convert_sql_pyspark.py, unchanged), so valid/test contain unseen feature combinations.

Usage:  python build.py [--train 10000 --valid 500 --test 300 --mix-old 7000]
"""
from __future__ import annotations

import argparse
import json
import random
import sys
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from convert_sql_pyspark import DEFAULT_SYSTEM_PROMPT, format_sample, norm_sql, signature, split, write_jsonl  # noqa: E402

EVAL_SETS = ["holdout.jsonl", "holdout_v4.jsonl", "real_queries.jsonl", "test_fixed.jsonl"]
BASE = "mlx-community/Qwen2.5-Coder-7B-Instruct-4bit"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--src", default="raw_pairs")
    ap.add_argument("--train", type=int, default=10000)
    ap.add_argument("--valid", type=int, default=500)
    ap.add_argument("--test", type=int, default=300)
    ap.add_argument("--max-tokens", type=int, default=3000)
    ap.add_argument("--mix-old", type=int, default=7000, help="v7 training rows added to data_mix/ (0 = skip)")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    rng = random.Random(args.seed)

    recs = [json.loads(line) for line in (HERE / args.src / "verified.jsonl").read_text().splitlines() if line.strip()]
    excluded = set()
    for name in EVAL_SETS:
        p = HERE.parent.parent / "eval" / name
        if p.exists():
            excluded |= {norm_sql(json.loads(line)["sql"]) for line in p.read_text().splitlines() if line.strip()}
    n0 = len(recs)
    recs = [r for r in recs if norm_sql(r["sql"]) not in excluded]
    n_eval = n0 - len(recs)

    from transformers import AutoTokenizer
    tok = AutoTokenizer.from_pretrained(BASE, local_files_only=True)

    def n_tokens(r) -> int:
        text = tok.apply_chat_template(format_sample(r["sql"], r["code"], DEFAULT_SYSTEM_PROMPT)["messages"],
                                       tokenize=False)
        return len(tok.encode(text))

    for r in recs:
        r["n_tokens"] = n_tokens(r)
    too_long = [r for r in recs if r["n_tokens"] > args.max_tokens]
    recs = [r for r in recs if r["n_tokens"] <= args.max_tokens]

    need = args.train + args.valid + args.test
    nonempty = [r for r in recs if r["n_rows"] > 0]
    empty = [r for r in recs if r["n_rows"] == 0]
    rng.shuffle(empty)
    pool = nonempty + empty[: max(0, need - len(nonempty))]
    if len(pool) < need:
        raise SystemExit(f"only {len(pool)} usable pairs, need {need} - generate more")
    rng.shuffle(pool)
    pool = pool[: int(need * 1.04)]           # a little slack: split() keeps signatures whole

    total = len(pool)
    train, valid, test = split(pool, args.valid / total * 1.02, args.test / total * 1.02, 30, args.seed)
    rng.shuffle(train)
    train, valid, test = train[: args.train], valid[: args.valid], test[: args.test]
    if len(train) < args.train or len(valid) < args.valid or len(test) < args.test:
        raise SystemExit(f"split too small: train {len(train)} valid {len(valid)} test {len(test)}")

    out = HERE / "data"
    out.mkdir(exist_ok=True)
    for name, rows in (("train", train), ("valid", valid), ("test", test)):
        write_jsonl(out / f"{name}.jsonl", [format_sample(r["sql"], r["code"], DEFAULT_SYSTEM_PROMPT) for r in rows])
    for name, rows in (("valid", valid), ("test", test)):
        write_jsonl(out / f"{name}_meta.jsonl", [{k: r[k] for k in ("id", "family", "features", "order_cols", "sql", "code")}
                                                  for r in rows])
    toks = sorted(r["n_tokens"] for r in train)
    sig = {n: {signature(r) for r in rows} for n, rows in (("train", train), ("valid", valid), ("test", test))}
    stats = {
        "verified_pairs": n0, "excluded_eval_overlap": n_eval, "dropped_too_long": len(too_long),
        "train": len(train), "valid": len(valid), "test": len(test),
        "empty_result_pairs": {n: sum(r["n_rows"] == 0 for r in rows) for n, rows in
                               (("train", train), ("valid", valid), ("test", test))},
        "train_tokens": {"mean": round(sum(toks) / len(toks)), "p50": toks[len(toks) // 2],
                         "p95": toks[int(len(toks) * .95)], "max": toks[-1]},
        "signature_overlap_train_valid": len(sig["train"] & sig["valid"]),
        "signature_overlap_train_test": len(sig["train"] & sig["test"]),
        "family_counts": {n: dict(Counter(r["family"] for r in rows)) for n, rows in
                          (("train", train), ("valid", valid), ("test", test))},
        "top_features_train": dict(Counter(f for r in train for f in r["features"]).most_common(40)),
    }
    (out / "split_stats.json").write_text(json.dumps(stats, indent=2))

    if args.mix_old:
        old_dir = HERE.parent / "data"
        old_train = [line for line in (old_dir / "train.jsonl").read_text().splitlines() if line.strip()]
        old_valid = [line for line in (old_dir / "valid.jsonl").read_text().splitlines() if line.strip()]
        mix = HERE / "data_mix"
        mix.mkdir(exist_ok=True)
        new_train = (out / "train.jsonl").read_text().splitlines()
        new_valid = (out / "valid.jsonl").read_text().splitlines()
        mixed = new_train + rng.sample(old_train, min(args.mix_old, len(old_train)))
        rng.shuffle(mixed)
        mvalid = new_valid + rng.sample(old_valid, min(len(new_valid), len(old_valid)))
        rng.shuffle(mvalid)
        (mix / "train.jsonl").write_text("\n".join(mixed) + "\n")
        (mix / "valid.jsonl").write_text("\n".join(mvalid) + "\n")
        (mix / "test.jsonl").write_text((out / "test.jsonl").read_text())
        stats["data_mix"] = {"train": len(mixed), "new": len(new_train), "from_v7": len(mixed) - len(new_train),
                             "valid": len(mvalid)}
        (out / "split_stats.json").write_text(json.dumps(stats, indent=2))
    print(json.dumps({k: v for k, v in stats.items() if k not in ("family_counts", "top_features_train")}, indent=2))


if __name__ == "__main__":
    main()
