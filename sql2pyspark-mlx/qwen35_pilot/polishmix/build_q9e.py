"""Qwen3.5-9B run q9e data (data_q9e): a FRESH run from the 4-bit base on one combined mix (owner 2026-11-22).

Goal: pass the promotion rule vs v18a-q4. q9b -> q9c -> q9d each continued the previous adapter; every round fixed its
targets and flipped 1-2 other cases (hold-out 025 passes in q9a / q9b, loops in q9c / q9d). q9e trains everything at
once instead, from the base, so no later round can pull the model away from what an earlier one taught.
Train = q9a's data (data_q9a: the v18a mix, 8,000 pairs with renamed tables) + every NEW pair of data_q9b, data_q9c and
data_q9d (their replay lines, which are copies of v18a-mix pairs, are dropped), de-duplicated by SQL text. Every new
pair was round-trip checked and Spark spot-checked when its round was built; this script adds:
  - provenance: every train line is byte-identical to a line of data_q9a / data_q9b / data_q9c / data_q9d
  - leakage: no eval SQL (hold-out, v4, fixed, complex test, schema-shift, complex-shift, v11-v14 held-out) in train
  - valid / train overlap = 0 (valid = data_q9c/valid.jsonl, 737, as for q9c / q9d)
  - Spark spot-check (--spot N, default 200) of pairs whose tables the seeded checker knows: SQL and code agree
  .venv/bin/python qwen35_pilot/polishmix/build_q9e.py            (from sql2pyspark-mlx)
"""
from __future__ import annotations

import argparse
import json
import random
import re
import sys
from collections import Counter
from pathlib import Path

MLX = Path(__file__).resolve().parents[2]
P = MLX / "qwen35_pilot"
sys.path.insert(0, str(MLX))
OUT = P / "data_q9e"
SEED = 2032
lines = lambda p: [x for x in Path(p).read_text(encoding="utf-8").splitlines() if x.strip()]
sql_of = lambda x: json.loads(x)["messages"][1]["content"]
code_of = lambda x: json.loads(x)["messages"][2]["content"]
norm = lambda s: " ".join(s.split()).lower()


def leakage(train: list[str]) -> tuple[int, int]:
    evs = [MLX.parent / "eval" / f for f in ("holdout.jsonl", "holdout_v4.jsonl", "test_fixed.jsonl",
                                             "complex_v8_test.jsonl")]
    evs += [MLX / "complex_v16/data/complex_shift_meta.jsonl", MLX / "schema_v15/data/schema_shift_meta.jsonl"]
    evs += [MLX / f"targeted_v{v}/data/heldout_meta.jsonl" for v in (11, 12, 13, 14)]
    ev_sql = set()
    for f in evs:
        for x in lines(f):
            d = json.loads(x)
            s = d.get("sql") or d.get("input") or (d["messages"][1]["content"] if "messages" in d else "")
            ev_sql.add(norm(s))
    big = [e for e in ev_sql if len(e) > 40]
    norm_train = {norm(sql_of(x)) for x in train}
    return len(ev_sql), sum(1 for t in norm_train if any(e in t for e in big))


def spot_check(train: list[str], n: int, rng: random.Random) -> dict:
    """Run n random pairs (all tables known to the seeded checker) as SQL and as code; count agreements."""
    from verify_pairs import Checker, compare, rows_of
    ck = Checker()
    known = set(ck.frames)
    cands = [x for x in train if (t := set(re.findall(r"\bdf_(\w+)", code_of(x))))
             and all(f"df_{y}" in known for y in t)]
    stats, bad = Counter(), []
    unresolved = re.compile(r"UNRESOLVED_COLUMN\S* .*?name `(?:\w+`\.`)?(\w+)` cannot be resolved")

    def stem_of(name: str, frames: dict) -> str | None:
        """Seeded column that `name` extends (q9c / q9d stem pairs: X -> X_w, w_X, X_w_w2), longest match first."""
        cols = {c for df in frames.values() for c in df.columns}
        hits = [c for c in cols if name != c and (name.startswith(c + "_") or name.endswith("_" + c))]
        return max(hits, key=len) if hits else None

    for x in rng.sample(cands, min(n, len(cands))):
        rec = json.loads(x)
        sql = re.search(r"```sql\n(.*?)```", rec["messages"][1]["content"], flags=re.S).group(1)
        code = re.search(r"```python\n(.*?)```", rec["messages"][2]["content"], flags=re.S).group(1)
        frames, renamed = dict(ck.frames), {}
        for _attempt in range(5):        # stem pairs: rename the seeded column the same way, then run again
            try:
                for name, df in frames.items():
                    df.createOrReplaceTempView(name[3:])
                s = ck.spark.sql(sql.strip().rstrip(";"))
                ns = dict(ck.base_ns)
                ns.update(frames)
                exec(compile(code, "<pyspark>", "exec"), ns)
                res = ns["result"]
                ok, err = compare(s.columns, rows_of(s), res.columns, rows_of(res)).ok, ""
                break
            except Exception as e:  # noqa: BLE001
                ok, err = False, str(e)[:300]
                m = unresolved.search(str(e))
                c = stem_of(m.group(1), frames) if m else None
                if not c:
                    break
                renamed[c] = m.group(1)
                frames = {k: (df.withColumnRenamed(c, m.group(1)) if c in df.columns else df)
                          for k, df in frames.items()}
        for name, df in ck.frames.items():   # restore the seeded views
            df.createOrReplaceTempView(name[3:])
        stats["checked"] += 1
        stats["ok"] += ok
        stats["stem_columns_renamed_for_check"] += bool(renamed)
        if not ok:
            bad.append({"line": x, "error": err, "renamed": renamed})
    stats["candidates"] = len(cands)
    if bad:
        (OUT / "spot_failures.jsonl").write_text("".join(json.dumps(b) + "\n" for b in bad))
    return dict(stats)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--spot", type=int, default=200)
    a = ap.parse_args()
    rng = random.Random(SEED)

    mix = set(lines(MLX / "complex_v17" / "data_mix" / "train.jsonl"))
    base = lines(P / "data_q9a" / "train.jsonl")
    parts: dict[str, list[str]] = {"q9a_base": base}
    seen_sql = {sql_of(x) for x in base}
    dropped = Counter()
    for r in ("q9b", "q9c", "q9d"):
        keep = []
        for x in lines(P / f"data_{r}" / "train.jsonl"):
            if x in mix:
                dropped[f"{r}_replay"] += 1
                continue
            s = sql_of(x)
            if s in seen_sql:
                dropped[f"{r}_duplicate_sql"] += 1
                continue
            seen_sql.add(s)
            keep.append(x)
        parts[f"{r}_new"] = keep
    train = [x for v in parts.values() for x in v]
    rng.shuffle(train)
    valid = lines(P / "data_q9c" / "valid.jsonl")

    n_ev, hits = leakage(train)
    tr_sql = {norm(sql_of(x)) for x in train}
    overlap = sum(1 for x in valid if norm(sql_of(x)) in tr_sql)
    sources = set(base)
    for r in ("q9b", "q9c", "q9d"):
        sources |= set(lines(P / f"data_{r}" / "train.jsonl"))
    not_from_source = sum(1 for x in train if x not in sources)
    chars = sorted(len(x) for x in train)
    print(f"eval SQL strings checked: {n_ev}; inside train: {hits}; valid/train overlap: {overlap}; "
          f"lines not from a source file: {not_from_source}")

    OUT.mkdir(exist_ok=True)
    (OUT / "train.jsonl").write_text("\n".join(train) + "\n")
    (OUT / "valid.jsonl").write_text("\n".join(valid) + "\n")
    stats = {k: len(v) for k, v in parts.items()} | {"train": len(train), "valid": len(valid),
                                                     "dropped": dict(dropped),
                                                     "eval_sql_inside_train": hits,
                                                     "valid_train_overlap": overlap,
                                                     "lines_not_from_source": not_from_source,
                                                     "max_line_chars": chars[-1],
                                                     "mean_line_chars": round(sum(chars) / len(chars))}
    if a.spot:
        stats["spot_check"] = spot_check(train, a.spot, rng)
    (OUT / "stats.json").write_text(json.dumps(stats, indent=2) + "\n")
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
