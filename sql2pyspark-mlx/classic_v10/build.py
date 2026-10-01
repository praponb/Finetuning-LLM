"""Build the v10 training data: the classic v7 pairs in the "mirror" window style + targeted IN / NOT IN pairs.

  python build.py prepare     ../raw_pairs/verified.jsonl -> raw_pairs/ (only the pairs whose code mirror.py changed,
                              to be re-verified in Spark with ../verify_pairs.py)
  python build.py split       -> data/ (the v7 split with mirrored code) and data_mix/ (what v10 trains on)

data/ keeps the v7 train / valid / test membership (by SQL), so ../data/test.jsonl stays held out and test_fixed
stays excluded. data_mix/ = every classic training pair whose code changed (windows) + the verified IN / NOT IN
pairs (raw_inlist/, all but --inlist-valid) + --plain classic pairs without windows + --complex complex_v9 pairs
(already mirror style), so v10 keeps what v9 learned.
"""
from __future__ import annotations

import argparse
import json
import random
import sys
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
MLX = HERE.parent
sys.path.insert(0, str(MLX))
sys.path.insert(0, str(HERE))

from convert_sql_pyspark import DEFAULT_SYSTEM_PROMPT, format_sample, norm_sql, write_jsonl  # noqa: E402
from mirror import mirror_code  # noqa: E402

META_KEYS = ("id", "family", "features", "order_cols", "sql", "code")


def read_jsonl(p: Path) -> list:
    return [json.loads(line) for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]


def chat_sql(row: dict) -> str:
    text = row["messages"][1]["content"]
    return norm_sql(text.split("```sql\n", 1)[1].rsplit("\n```", 1)[0])


def prepare():
    out = HERE / "raw_pairs"
    out.mkdir(exist_ok=True)
    metas, stats = [], Counter()
    for r in read_jsonl(MLX / "raw_pairs" / "verified.jsonl"):
        new = mirror_code(r["sql"], r["code"])
        if new is None:
            stats["skipped"] += 1
            continue
        if new == r["code"]:
            stats["unchanged"] += 1
            continue
        stats["changed"] += 1
        (out / f"{r['id']}.sql").write_text(r["sql"].rstrip("\n") + "\n")
        (out / f"{r['id']}.py").write_text(new)
        metas.append({k: r[k] for k in ("id", "family", "features", "order_cols")})
    with open(out / "meta_mirror.jsonl", "w") as f:
        for m in metas:
            f.write(json.dumps(m) + "\n")
    print(dict(stats))


def split(args):
    rng = random.Random(args.seed)
    old = {r["id"]: r for r in read_jsonl(MLX / "raw_pairs" / "verified.jsonl")}
    changed = {r["id"]: r for r in read_jsonl(HERE / "raw_pairs" / "verified.jsonl")}
    failed = [r["id"] for r in read_jsonl(HERE / "raw_pairs" / "verify_failures.jsonl")] \
        if (HERE / "raw_pairs" / "verify_failures.jsonl").exists() else []
    by_sql = {}
    for i, r in old.items():
        if i in failed:
            continue                                   # mirrored code no longer matches: drop the pair
        rec = dict(changed.get(i, r))
        rec["mirrored"] = i in changed
        by_sql[norm_sql(r["sql"])] = rec
    parts = {}
    for n in ("train", "valid", "test"):
        parts[n] = [by_sql[s] for s in (chat_sql(x) for x in read_jsonl(MLX / "data" / f"{n}.jsonl")) if s in by_sql]
    inlist = [r for r in read_jsonl(HERE / "raw_inlist" / "verified.jsonl") if r["n_rows"] > 0]
    rng.shuffle(inlist)
    in_valid, in_train = inlist[: args.inlist_valid], inlist[args.inlist_valid:]

    out = HERE / "data"
    out.mkdir(exist_ok=True)
    full_train = parts["train"] + in_train
    rng.shuffle(full_train)
    for n, rows in (("train", full_train), ("valid", parts["valid"] + in_valid), ("test", parts["test"])):
        write_jsonl(out / f"{n}.jsonl", [format_sample(r["sql"], r["code"], DEFAULT_SYSTEM_PROMPT) for r in rows])
    write_jsonl(out / "test_meta.jsonl", [{k: r[k] for k in META_KEYS} for r in parts["test"]])
    write_jsonl(out / "inlist_valid_meta.jsonl", [{k: r[k] for k in META_KEYS} for r in in_valid])

    mirrored = [r for r in parts["train"] if r["mirrored"]]
    plain = [r for r in parts["train"] if not r["mirrored"]]
    mix = mirrored + in_train + rng.sample(plain, min(args.plain, len(plain)))
    lines = [json.dumps(format_sample(r["sql"], r["code"], DEFAULT_SYSTEM_PROMPT), ensure_ascii=False) for r in mix]
    cx = (MLX / "complex_v9" / "data" / "train.jsonl").read_text().splitlines()
    lines += rng.sample(cx, min(args.complex, len(cx)))
    rng.shuffle(lines)
    cv_lines = (MLX / "complex_v9" / "data" / "valid.jsonl").read_text().splitlines()
    valid_classic = rng.sample(parts["valid"], min(args.valid_classic, len(parts["valid"])))
    vlines = [json.dumps(format_sample(r["sql"], r["code"], DEFAULT_SYSTEM_PROMPT), ensure_ascii=False)
              for r in valid_classic + in_valid] + rng.sample(cv_lines, min(300, len(cv_lines)))
    rng.shuffle(vlines)
    mdir = HERE / "data_mix"
    mdir.mkdir(exist_ok=True)
    (mdir / "train.jsonl").write_text("\n".join(lines) + "\n")
    (mdir / "valid.jsonl").write_text("\n".join(vlines) + "\n")
    stats = {
        "v7_pairs": len(old), "mirrored_verified": len(changed), "mirror_failures": len(failed),
        "split": {n: len(v) for n, v in parts.items()},
        "inlist": {"verified_non_empty": len(inlist), "train": len(in_train), "valid": len(in_valid)},
        "data_mix": {"train": len(lines), "classic_mirrored": len(mirrored), "inlist": len(in_train),
                     "classic_plain": min(args.plain, len(plain)), "complex_v9": min(args.complex, len(cx)),
                     "valid": len(vlines)},
        "train_families": dict(Counter(r["family"] for r in mix).most_common()),
    }
    (out / "split_stats.json").write_text(json.dumps(stats, indent=2))
    print(json.dumps({k: v for k, v in stats.items() if k != "train_families"}, indent=2))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("step", choices=["prepare", "split"])
    ap.add_argument("--plain", type=int, default=2000)
    ap.add_argument("--complex", type=int, default=3000)
    ap.add_argument("--inlist-valid", type=int, default=50)
    ap.add_argument("--valid-classic", type=int, default=500)
    ap.add_argument("--seed", type=int, default=10)
    args = ap.parse_args()
    prepare() if args.step == "prepare" else split(args)


if __name__ == "__main__":
    main()
