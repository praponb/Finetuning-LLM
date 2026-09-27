"""Turn verified SQL/PySpark pairs into MLX chat-format JSONL (train / valid / test).

Input is raw_pairs/verified.jsonl written by verify_pairs.py, so only pairs
whose two sides produce identical results are used.

Split: pairs are grouped by *feature signature* (family + sorted feature tags).
Whole signatures are held out for valid/test, so those sets contain feature
combinations the model never saw during training. A signature is only held
out if every one of its features still has plenty of training examples.
Hand-written pairs always go to train.

The chat format (system prompt + "Convert SQL to PySpark:" user message) is the
same one Modelfile and app.py use at inference time — keep them in sync.

Usage:  python convert_sql_pyspark.py --src raw_pairs --out-dir data
"""
from __future__ import annotations

import argparse
import json
import random
import re
from collections import Counter, defaultdict
from pathlib import Path

DEFAULT_SYSTEM_PROMPT = (
    "You are an expert data engineer specializing in SQL and Apache Spark. "
    "Convert the provided SQL query into clean, idiomatic PySpark DataFrame code."
)


def format_sample(sql_text: str, py_text: str, system_prompt: str) -> dict:
    """Chat-template record in the format mlx_lm.lora expects."""
    return {
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Convert SQL to PySpark:\n```sql\n{sql_text}\n```"},
            {"role": "assistant", "content": f"```python\n{py_text}\n```"},
        ]
    }


def write_jsonl(file_path: Path, data: list):
    with open(file_path, "w", encoding="utf-8") as f:
        for item in data:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")


def norm_sql(sql: str) -> str:
    return re.sub(r"\s+", " ", sql.lower()).strip().rstrip(";")


def signature(rec) -> str:
    return rec.get("family", "?") + "|" + ",".join(sorted(rec.get("features", [])))


def split(recs, valid_frac, test_frac, min_train_per_feature, seed):
    rng = random.Random(seed)
    fixed_train = [r for r in recs if r.get("family") == "handwritten"]
    pool = [r for r in recs if r.get("family") != "handwritten"]
    groups = defaultdict(list)
    for r in pool:
        groups[signature(r)].append(r)
    train_feat = Counter(f for r in pool for f in r.get("features", []))
    sigs = list(groups)
    rng.shuffle(sigs)
    targets = {"valid": valid_frac * len(pool), "test": test_frac * len(pool)}
    held = {"valid": [], "test": []}
    train = list(fixed_train)
    for sig in sigs:
        rows = groups[sig]
        feats = Counter(f for r in rows for f in r.get("features", []))
        dest = next((d for d in ("valid", "test") if len(held[d]) + len(rows) <= targets[d] * 1.05), None)
        # keep big signatures in train (they are the bread and butter) and never starve a feature
        if dest and len(rows) <= 0.02 * len(pool) and all(train_feat[f] - n >= min_train_per_feature
                                                           for f, n in feats.items()):
            held[dest] += rows
            train_feat.subtract(feats)
        else:
            train += rows
    return train, held["valid"], held["test"]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--src", default="raw_pairs", help="folder containing verified.jsonl")
    ap.add_argument("--out-dir", default="data")
    ap.add_argument("--valid-frac", type=float, default=0.10)
    ap.add_argument("--test-frac", type=float, default=0.05)
    ap.add_argument("--min-train-per-feature", type=int, default=30)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--system-prompt", default=DEFAULT_SYSTEM_PROMPT)
    ap.add_argument("--exclude", nargs="*", default=[],
                    help="JSONL files whose `sql` must never appear in any split (e.g. eval/test_fixed.jsonl)")
    args = ap.parse_args()

    src = Path(args.src) / "verified.jsonl"
    if not src.exists():
        raise SystemExit(f"{src} not found - run verify_pairs.py first")
    recs = [json.loads(line) for line in src.read_text(encoding="utf-8").splitlines() if line.strip()]
    excluded = {norm_sql(json.loads(line)["sql"]) for f in args.exclude
                for line in Path(f).read_text(encoding="utf-8").splitlines() if line.strip()}
    before = len(recs)
    recs = [r for r in recs if norm_sql(r["sql"]) not in excluded]
    if args.exclude:
        print(f"excluded {before - len(recs)} pairs that match {len(excluded)} queries in {args.exclude}")
    train, valid, test = split(recs, args.valid_frac, args.test_frac, args.min_train_per_feature, args.seed)
    random.Random(args.seed).shuffle(train)

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    for name, rows in (("train", train), ("valid", valid), ("test", test)):
        write_jsonl(out / f"{name}.jsonl", [format_sample(r["sql"], r["code"], args.system_prompt) for r in rows])
    # keep ids + metadata of the held-out sets for execution-based evaluation
    for name, rows in (("valid", valid), ("test", test)):
        write_jsonl(out / f"{name}_meta.jsonl", [{k: r[k] for k in ("id", "family", "features", "order_cols", "sql",
                                                                      "code")} for r in rows])

    sig = {n: {signature(r) for r in rows} for n, rows in (("train", train), ("valid", valid), ("test", test))}
    stats = {
        "pairs": len(recs), "train": len(train), "valid": len(valid), "test": len(test),
        "signatures": {k: len(v) for k, v in sig.items()},
        "signature_overlap_train_valid": len(sig["train"] & sig["valid"]),
        "signature_overlap_train_test": len(sig["train"] & sig["test"]),
        "features_missing_from_train": sorted({f for r in valid + test for f in r["features"]}
                                              - {f for r in train for f in r["features"]}),
        "family_counts": {n: dict(Counter(r["family"] for r in rows))
                          for n, rows in (("train", train), ("valid", valid), ("test", test))},
    }
    (out / "split_stats.json").write_text(json.dumps(stats, indent=2))
    print(json.dumps({k: v for k, v in stats.items() if k != "family_counts"}, indent=2))


if __name__ == "__main__":
    main()
