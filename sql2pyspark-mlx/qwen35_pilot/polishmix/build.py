"""Qwen3.5-9B polish round q9b data (data_q9b), on top of the q9a adapter (owner 2026-11-20).

q9a-q4 (fresh QLoRA, 9,600 it) has the best complex score (93) but fails the promotion rule vs v18a-q4 by small margins.
Its failures that v18a does not have:
  complex-shift  column-name slips on unseen schemas (is_certified_used -> is_certified): table renames alone (namecopy)
                 never asked the model to copy a COLUMN name it has not seen
  v13 held-out   scalar subquery collected into a Python number, then .alias() on it
  v11 held-out   correlated EXISTS / IN: a condition on the outer alias pushed into the inner filter before the join
Train (12,000, shuffled, seed 2029):
  3,600 v18a-mix pairs renamed: tables (namecopy.rename) AND 1-3 columns per pair (new here)
  1,800 targeted_v14 pairs not in the mix (correlated + uncorrelated scalar subqueries side by side; not v13 alone)
  1,200 targeted_v11 pairs not in the mix (correlated EXISTS / IN)
  1,200 complex_v16 pairs not in the mix (synthetic catalog; complex-shift)
  1,200 complex_v9 pairs not in the mix (complex_v8 eval catalog; keep complex 93)
  3,000 other v18a-mix pairs (replay for hold-out / v4 / fixed)
Valid: data_q9a/valid.jsonl + 100 renamed mix-valid pairs + 50 targeted_v14 valid + 50 targeted_v11 valid.
Column renames are checked by a round trip (reverse mapping gives the original text back) and a Spark spot-check
(--spot N, default 200): the renamed pair must agree on seeded tables whose columns get the same renames, wherever the
original pair agrees.
  .venv/bin/python qwen35_pilot/polishmix/build.py            (from sql2pyspark-mlx)
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
sys.path.insert(0, str(P / "namecopy"))
sys.path.insert(0, str(MLX))
import build as namecopy  # noqa: E402  (qwen35_pilot/namecopy/build.py)

OUT = P / "data_q9b"
SEED = 2029
N = {"renamed": 3600, "targeted_v14": 1800, "targeted_v11": 1200, "complex_v16": 1200, "complex_v9": 1200,
     "replay": 3000}
TAGS = "amt qty flag code ts score level note kind rank tally ref mark grade span".split()
lines = lambda p: [x for x in Path(p).read_text(encoding="utf-8").splitlines() if x.strip()]
sql_of = lambda x: json.loads(x)["messages"][1]["content"]


def col_name(rng: random.Random) -> str:
    noun = rng.choice(namecopy.NOUNS).rstrip("s") or "item"
    if rng.random() < 0.5:
        return f"{rng.choice(namecopy.QUALS)}_{noun}"
    return f"{noun}_{rng.choice(TAGS)}"


def column_candidates(sql: str, code: str, tables: set[str]) -> list[str]:
    """Column names safe to rename: in the SQL and inside quoted strings in the code, with '_', >= 6 chars."""
    sql_lits = set(re.findall(r"[A-Za-z_]\w*", " ".join(re.findall(r"'[^']*'", sql))))
    code_strs = re.findall(r'"([^"]*)"', code)
    quoted = set(re.findall(r"[A-Za-z_]\w*", " ".join(code_strs)))
    code_bare = set(re.findall(r"[A-Za-z_]\w*", re.sub(r'"[^"]*"', '""', code)))
    sql_ids = set(re.findall(r"[A-Za-z_]\w*", re.sub(r"'[^']*'", "''", sql)))
    out = []
    for w in sorted(sql_ids & quoted):
        if "_" not in w or len(w) < 6 or w.startswith("df_") or w.lower() in tables:
            continue
        if w in sql_lits or w in code_bare:                      # literal value, or a Python name / function
            continue
        if re.search(rf"\b{w}\s*\(", sql + code):                 # function call
            continue
        exact = len(re.findall(rf"\b{w}\b", sql + code))
        if exact != len(re.findall(rf"\b{w}\b", sql + code, flags=re.I)):   # other casing somewhere: skip
            continue
        out.append(w)
    return out


def rename_columns(msgs: list[dict], rng: random.Random, banned: set[str]) -> tuple[list[dict], dict] | None:
    user, code = msgs[1]["content"], msgs[2]["content"]
    m = re.search(r"```sql\n(.*?)```", user, flags=re.S)
    if not m:
        return None
    tables = {t.lower() for t in re.findall(r"\bdf_(\w+)", code)}
    cands = column_candidates(m.group(1), code, tables)
    if not cands:
        return None
    present = namecopy.identifiers(user + " " + code)
    mapping: dict[str, str] = {}
    for c in rng.sample(cands, rng.randint(1, min(3, len(cands)))):
        for _ in range(50):
            y = col_name(rng)
            if y not in banned and y not in present and y not in mapping.values():
                mapping[c] = y
                break
    if not mapping:
        return None
    sub = lambda s, mp: [s := re.sub(rf"\b{a}\b", b, s) for a, b in mp.items()][-1]
    u2, c2 = sub(user, mapping), sub(code, mapping)
    rev = {b: a for a, b in mapping.items()}
    if sub(u2, rev) != user or sub(c2, rev) != code:             # round trip must restore the original exactly
        return None
    out = [dict(x) for x in msgs]
    out[1]["content"], out[2]["content"] = u2, c2
    return out, mapping


def renamed_pair(rec: dict, rng: random.Random, banned: set[str]):
    """Table rename (namecopy) then column rename; returns (record, table_map, col_map) or None."""
    orig = rec["messages"]
    m = re.search(r"```sql\n(.*?)```", orig[1]["content"], flags=re.S)
    if not m:
        return None
    tabs = namecopy.tables_of(m.group(1), orig[2]["content"])
    t_msgs = namecopy.rename(orig, rng, banned)
    if not t_msgs or not tabs:
        return None
    # recover the table map from the code: df_<old> positions map to df_<new> positions
    olds = re.findall(r"\bdf_(\w+)", orig[2]["content"])
    news = re.findall(r"\bdf_(\w+)", t_msgs[2]["content"])
    if len(olds) != len(news):
        return None
    tmap = {}
    for a, b in zip(olds, news):
        if tmap.setdefault(a, b) != b:
            return None
    r = rename_columns(t_msgs, rng, banned)
    if not r:
        return None
    msgs, cmap = r
    return {"messages": msgs}, tmap, cmap


def spot_check(samples: list[tuple[dict, dict, dict, dict]], n: int) -> dict:
    """Run original and renamed pairs on seeded tables; count failures caused by the renames."""
    from verify_pairs import Checker, compare, rows_of
    ck = Checker()
    known = set(ck.frames)
    todo = [s for s in samples if all(f"df_{t}" in known for t in s[2])][:n]
    stats = Counter()
    bad = []
    for orig, new, tmap, cmap in todo:
        def run(rec, frames):
            sql = re.search(r"```sql\n(.*?)```", rec["messages"][1]["content"], flags=re.S).group(1)
            code = re.search(r"```python\n(.*?)```", rec["messages"][2]["content"], flags=re.S).group(1)
            for name, df in frames.items():
                df.createOrReplaceTempView(name[3:])
            s = ck.spark.sql(sql.strip().rstrip(";"))
            ns = dict(ck.base_ns)
            ns.update(frames)
            exec(compile(code, "<pyspark>", "exec"), ns)
            res = ns["result"]
            return compare(s.columns, rows_of(s), res.columns, rows_of(res))
        try:
            ok_orig = run(orig, {f"df_{t}": ck.frames[f"df_{t}"] for t in tmap}).ok
        except Exception:
            ok_orig = False
        frames = {}
        for t, y in tmap.items():
            df = ck.frames[f"df_{t}"]
            for a, b in cmap.items():
                if a in df.columns:
                    df = df.withColumnRenamed(a, b)
            frames[f"df_{y}"] = df
        try:
            ok_new = run(new, frames).ok
        except Exception as e:  # noqa: BLE001
            ok_new, err = False, str(e)[:200]
        for t in tmap:                                         # restore the original views
            ck.frames[f"df_{t}"].createOrReplaceTempView(t)
        stats["checked"] += 1
        stats["orig_ok"] += ok_orig
        stats["renamed_ok"] += ok_new
        if ok_orig and not ok_new:
            stats["rename_caused_fail"] += 1
            bad.append({"orig": orig, "new": new, "cols": cmap})
    if bad:
        (OUT / "spot_failures.jsonl").write_text("".join(json.dumps(b) + "\n" for b in bad))
    return dict(stats)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--spot", type=int, default=200, help="renamed pairs to check on Spark (0 = skip)")
    a = ap.parse_args()
    rng = random.Random(SEED)
    banned = namecopy.eval_identifiers()

    mix = lines(MLX / "complex_v17" / "data_mix" / "train.jsonl")
    mix_sql = {sql_of(x) for x in mix}
    pools = {}
    for name, d in [("targeted_v14", "targeted_v14"), ("targeted_v11", "targeted_v11"),
                    ("complex_v16", "complex_v16"), ("complex_v9", "complex_v9")]:
        pools[name] = [x for x in lines(MLX / d / "data" / "train.jsonl") if sql_of(x) not in mix_sql]
    special = set()
    for d in ["complex_v8", "complex_v9", "complex_v16", "targeted_v11", "targeted_v12", "targeted_v13", "targeted_v14"]:
        special |= {sql_of(x) for x in lines(MLX / d / "data" / "train.jsonl")}
    replay_pool = [x for x in mix if sql_of(x) not in special]
    print("pools:", {k: len(v) for k, v in pools.items()}, "replay", len(replay_pool))

    # renamed part (tables + columns), from shuffled mix pairs
    order = list(range(len(mix)))
    rng.shuffle(order)
    renamed, spot_samples, used, ncols = [], [], set(), Counter()
    for i in order:
        if len(renamed) == N["renamed"]:
            break
        orig = json.loads(mix[i])
        r = renamed_pair(orig, rng, banned)
        if r:
            rec, tmap, cmap = r
            renamed.append(json.dumps(rec))
            spot_samples.append((orig, rec, tmap, cmap))
            used.add(mix[i])
            ncols[len(cmap)] += 1
    print(f"renamed {len(renamed)} (tried {order.index(i) + 1}); columns renamed per pair: {dict(sorted(ncols.items()))}")

    parts = {"renamed": renamed}
    for k in ["targeted_v14", "targeted_v11", "complex_v16", "complex_v9"]:
        parts[k] = rng.sample(pools[k], N[k])
    parts["replay"] = rng.sample([x for x in replay_pool if x not in used], N["replay"])
    train = [x for v in parts.values() for x in v]
    rng.shuffle(train)

    # valid: q9a valid + 100 renamed mix-valid + 50 v14 + 50 v11
    mvalid = [json.loads(x) for x in lines(MLX / "complex_v17" / "data_mix" / "valid.jsonl")]
    rng.shuffle(mvalid)
    vren = []
    for rec in mvalid:
        r = renamed_pair(rec, rng, banned)
        if r:
            vren.append(json.dumps(r[0]))
        if len(vren) == 100:
            break
    valid = (lines(P / "data_q9a" / "valid.jsonl") + vren
             + rng.sample(lines(MLX / "targeted_v14" / "data" / "valid.jsonl"), 50)
             + rng.sample(lines(MLX / "targeted_v11" / "data" / "valid.jsonl"), 50))

    # leakage: no eval SQL inside any train pair
    evs = [MLX.parent / "eval" / f for f in ("holdout.jsonl", "holdout_v4.jsonl", "test_fixed.jsonl", "complex_v8_test.jsonl")]
    evs += [MLX / "complex_v16/data/complex_shift_meta.jsonl", MLX / "schema_v15/data/schema_shift_meta.jsonl"]
    evs += [MLX / f"targeted_v{v}/data/heldout_meta.jsonl" for v in (11, 12, 13, 14)]
    ev_sql = set()
    for f in evs:
        for x in lines(f):
            d = json.loads(x)
            s = d.get("sql") or d.get("input") or (d["messages"][1]["content"] if "messages" in d else "")
            ev_sql.add(" ".join(s.split()).lower())
    norm_train = {" ".join(sql_of(x).split()).lower() for x in train}
    hits = sum(1 for t in norm_train if any(len(e) > 40 and e in t for e in ev_sql))
    print(f"eval SQL strings checked: {len(ev_sql)}; inside train: {hits}")

    OUT.mkdir(exist_ok=True)
    (OUT / "train.jsonl").write_text("\n".join(train) + "\n")
    (OUT / "valid.jsonl").write_text("\n".join(valid) + "\n")
    (OUT / "samples_renamed.jsonl").write_text(
        "".join(json.dumps({"tables": t, "columns": c, "sql": n["messages"][1]["content"], "code": n["messages"][2]["content"]}) + "\n"
                for _, n, t, c in spot_samples[:10]))
    stats = {k: len(v) for k, v in parts.items()} | {"train": len(train), "valid": len(valid),
                                                     "valid_renamed": len(vren), "eval_sql_inside_train": hits,
                                                     "columns_per_renamed_pair": dict(ncols)}
    if a.spot:
        stats["spot_check"] = spot_check(spot_samples, a.spot)
    (OUT / "stats.json").write_text(json.dumps(stats, indent=2) + "\n")
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
