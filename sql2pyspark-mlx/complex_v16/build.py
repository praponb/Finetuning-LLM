"""Build the v16 training data: the complex families on the synthetic catalog (raw_v16/verified.jsonl) plus a replay
of the earlier data, and a complex schema-shift test set on the 5 domains training never sees (raw_v16_shift).

  python build.py     -> data/ (train / valid / complex_shift_meta.jsonl) and data_mix/ (what v16 trains on)

Pairs longer than --max-tokens (Qwen2.5 chat template, as complex_v8/build.py) are dropped, as are empty results and
SQL already in an eval set. Per family (seeded shuffle) VALID_SHARE goes to validation, the rest up to --train in
proportion to what is available; the shift set takes --shift pairs spread over the families.
data_mix/ = the new pairs + --v15 schema_v15 + --v14 targeted_v14 + every classic scalar-subquery pair + --classic
other classic (of them --classic-or with OR) + --complex complex_v9 + --v12 targeted_v12 + --v11 targeted_v11.
"""
from __future__ import annotations

import argparse
import json
import random
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
MLX = HERE.parent
ROOT = MLX.parent
sys.path.insert(0, str(MLX))

from convert_sql_pyspark import DEFAULT_SYSTEM_PROMPT, format_sample, norm_sql, write_jsonl  # noqa: E402

BASE = "mlx-community/Qwen2.5-Coder-7B-Instruct-4bit"
META_KEYS = ("id", "family", "features", "order_cols", "sql", "code")
EVAL_SETS = ["eval/holdout.jsonl", "eval/holdout_v4.jsonl", "eval/real_queries.jsonl", "eval/test_fixed.jsonl",
             "eval/complex_v8_test.jsonl"]
OR_RE = re.compile(r"\bOR\b", re.I)
SCALAR_RE = re.compile(r"\(\s*SELECT\s+(AVG|MAX|MIN|SUM|COUNT)\(", re.I)


def read_jsonl(p: Path) -> list:
    return [json.loads(line) for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]


def chat_sql(line: str) -> str:
    return json.loads(line)["messages"][1]["content"]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--train", type=int, default=10000)
    ap.add_argument("--valid", type=int, default=150)
    ap.add_argument("--shift", type=int, default=300)
    ap.add_argument("--max-tokens", type=int, default=3000)
    ap.add_argument("--v15", type=int, default=2500)
    ap.add_argument("--v14", type=int, default=1500)
    ap.add_argument("--classic", type=int, default=3300)
    ap.add_argument("--classic-or", type=int, default=300)
    ap.add_argument("--complex", type=int, default=1500)
    ap.add_argument("--v12", type=int, default=800)
    ap.add_argument("--v11", type=int, default=700)
    ap.add_argument("--seed", type=int, default=16)
    args = ap.parse_args()
    rng = random.Random(args.seed)
    seen = {norm_sql(r["sql"]) for s in EVAL_SETS for r in read_jsonl(ROOT / s)}

    from transformers import AutoTokenizer
    tok = AutoTokenizer.from_pretrained(BASE)

    def n_tokens(r) -> int:
        text = tok.apply_chat_template(format_sample(r["sql"], r["code"], DEFAULT_SYSTEM_PROMPT)["messages"],
                                       tokenize=False)
        return len(tok.encode(text))

    stats = {}

    def usable(src):
        rows = read_jsonl(HERE / src / "verified.jsonl")
        st = {"verified": len(rows), "empty": sum(r["n_rows"] == 0 for r in rows)}
        rows = [r for r in rows if r["n_rows"] > 0 and norm_sql(r["sql"]) not in seen]
        for r in rows:
            r["n_tokens"] = n_tokens(r)
        st["too_long"] = sum(r["n_tokens"] > args.max_tokens for r in rows)
        rows = [r for r in rows if r["n_tokens"] <= args.max_tokens]
        st["usable"] = len(rows)
        return rows, st

    def by_family(rows):
        fam = defaultdict(list)
        for r in sorted(rows, key=lambda r: r["id"]):
            fam[r["family"]].append(r)
        for v in fam.values():
            rng.shuffle(v)
        return fam

    rows, stats["raw_v16"] = usable("raw_v16")
    if len(rows) < args.train + args.valid:
        raise SystemExit(f"only {len(rows)} usable pairs, need {args.train + args.valid}")
    fam = by_family(rows)
    total = len(rows)
    valid, train = [], []
    for f, rs in fam.items():
        nv = round(args.valid * len(rs) / total)
        valid += rs[:nv]
        train += rs[nv:]
    rng.shuffle(train)
    train = train[: args.train]
    srows, stats["raw_v16_shift"] = usable("raw_v16_shift")
    sfam = by_family(srows)
    shift = []
    while len(shift) < min(args.shift, len(srows)):       # round-robin over families
        for rs in sfam.values():
            if rs and len(shift) < args.shift:
                shift.append(rs.pop())

    out = HERE / "data"
    out.mkdir(exist_ok=True)
    fmt = lambda r: format_sample(r["sql"], r["code"], DEFAULT_SYSTEM_PROMPT)  # noqa: E731
    write_jsonl(out / "train.jsonl", [fmt(r) for r in train])
    write_jsonl(out / "valid.jsonl", [fmt(r) for r in valid])
    write_jsonl(out / "complex_shift_meta.jsonl", [{k: r[k] for k in META_KEYS} for r in shift])

    classic = (MLX / "classic_v10" / "data" / "train.jsonl").read_text().splitlines()
    classic_scalar = [x for x in classic if SCALAR_RE.search(chat_sql(x))]
    rest = [x for x in classic if not SCALAR_RE.search(chat_sql(x))]
    classic_or = [x for x in rest if OR_RE.search(chat_sql(x))]
    classic_plain = [x for x in rest if not OR_RE.search(chat_sql(x))]
    lines = [json.dumps(fmt(r), ensure_ascii=False) for r in train]
    lines += rng.sample((MLX / "schema_v15" / "data" / "train.jsonl").read_text().splitlines(), args.v15)
    lines += rng.sample((MLX / "targeted_v14" / "data" / "train.jsonl").read_text().splitlines(), args.v14)
    lines += classic_scalar
    lines += rng.sample(classic_or, args.classic_or)
    lines += rng.sample(classic_plain, args.classic - args.classic_or)
    lines += rng.sample((MLX / "complex_v9" / "data" / "train.jsonl").read_text().splitlines(), args.complex)
    lines += rng.sample((MLX / "targeted_v12" / "data" / "train.jsonl").read_text().splitlines(), args.v12)
    lines += rng.sample((MLX / "targeted_v11" / "data" / "train.jsonl").read_text().splitlines(), args.v11)
    rng.shuffle(lines)
    vlines = [json.dumps(fmt(r), ensure_ascii=False) for r in valid]
    vlines += rng.sample((MLX / "complex_v9" / "data" / "valid.jsonl").read_text().splitlines(), 50)
    vlines += [x for x in (MLX / "classic_v10" / "data" / "valid.jsonl").read_text().splitlines()
               if SCALAR_RE.search(chat_sql(x))]
    vlines += rng.sample((MLX / "schema_v15" / "data" / "valid.jsonl").read_text().splitlines(), 100)
    rng.shuffle(vlines)
    mdir = HERE / "data_mix"
    mdir.mkdir(exist_ok=True)
    (mdir / "train.jsonl").write_text("\n".join(lines) + "\n")
    (mdir / "valid.jsonl").write_text("\n".join(vlines) + "\n")

    toks = sorted(r["n_tokens"] for r in train)
    chars = [len(chat_sql(x)) + len(json.loads(x)["messages"][2]["content"]) for x in lines]
    stats["new_train_tokens"] = {"mean": round(sum(toks) / len(toks)), "p50": toks[len(toks) // 2],
                                 "p95": toks[int(len(toks) * 0.95)], "max": toks[-1]}
    stats["data_mix"] = {"train": len(lines), "new": len(train), "schema_v15": args.v15, "targeted_v14": args.v14,
                         "classic_scalar": len(classic_scalar), "classic": args.classic, "complex_v9": args.complex,
                         "targeted_v12": args.v12, "targeted_v11": args.v11, "valid": len(vlines),
                         "complex_shift": len(shift), "mean_chars": round(sum(chars) / len(chars)), "max_chars": max(chars)}
    stats["train_families"] = dict(Counter(r["family"] for r in train).most_common())
    stats["shift_families"] = dict(Counter(r["family"] for r in shift).most_common())
    (out / "split_stats.json").write_text(json.dumps(stats, indent=2))
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
