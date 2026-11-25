"""Qwen3.5-9B round q9c data (data_q9c), on top of the q9b adapter (owner 2026-11-21).

q9b-q4 is the best model overall but fails the promotion rule on complex-shift (122 vs v18a 126), hold-out (60 vs 61)
and v4 (19 vs 20). 20 of its 28 complex-shift failures are one slip: an unseen column that EXTENDS a familiar name
(is_certified_used -> is_certified; the mix has flag_is_certified 506x and derived names like flag_is_certified_label,
so "_used" is read as a derived suffix and dropped). q9b's renames used brand-new names and never showed a familiar
name with an unfamiliar extension. Hold-out 032 dropped a CONCAT '-' separator; v4 010 broke a join with INTERVAL
conditions; v4 011 invented F.lateral for LEFT JOIN LATERAL.
Train (seed 2030, shuffled; pools exclude the v18a mix AND data_q9b):
  3,600 stem extensions: 1-2 frequent mix columns get an extension (X_w, w_X, X_w_w2); tables also renamed in half
  1,200 plain table + column renames (q9b style)
  <=504 LEFT JOIN LATERAL (classic_v10), 600 CONCAT with short literals, 600 joins with INTERVAL conditions
  1,200 targeted v11 / v12 / v14 (400 each), 1,200 complex v16 / v9 (600 each), 3,000 v18a-mix replay
Valid: data_q9b/valid.jsonl + 100 stem-extension pairs from the mix's validation pairs.
  .venv/bin/python qwen35_pilot/polishmix/build_q9c.py            (from sql2pyspark-mlx)
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import random
import re
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
_spec = importlib.util.spec_from_file_location("polishmix_q9b", HERE / "build.py")
pm = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(pm)          # polishmix/build.py (q9b): renamed_pair, spot_check, helpers; imports namecopy
namecopy = pm.namecopy
MLX, P = pm.MLX, pm.P
OUT = P / "data_q9c"
SEED = 2030
MIN_FREQ = 20
EXT = ("used net raw adj est alt prev src gross base orig calc final peak eff ref lag init cur avg2 tot obs nom "
       "std hi lo ext pri sec").split()
KEYWORDS = set("""date timestamp string int integer bigint smallint tinyint double float decimal boolean long short byte
binary array map struct interval day days month months year years hour hours minute minutes second seconds week weeks
quarter true false null inner left right outer full cross semi anti asc desc first last nulls rows range unbounded
preceding following current row join select from where group order limit having union except intersect case when then
else end distinct over partition and not exists between like using lateral table values with""".split())
lines, sql_of = pm.lines, pm.sql_of
RE = {"lateral": re.compile(r"JOIN\s+LATERAL", re.I),
      "concat_lit": re.compile(r"CONCAT\s*\([^)]*'[^']{1,3}'", re.I),
      "interval_join": re.compile(r"\bON\b[^;]*INTERVAL", re.I | re.S)}


def mix_column_freq(mix: list[str]) -> Counter:
    freq = Counter()
    for x in mix:
        for s in re.findall(r'"([A-Za-z_][\w.]*)"', json.loads(x)["messages"][2]["content"]):
            freq[s.split(".")[-1]] += 1
    return freq


def stem_candidates(sql: str, code: str, tables: set[str], freq: Counter) -> list[str]:
    """Like polishmix.column_candidates but for frequent names (>= MIN_FREQ in the mix), '_' not required."""
    sql_lits = set(re.findall(r"[A-Za-z_]\w*", " ".join(re.findall(r"'[^']*'", sql))))
    quoted = set(re.findall(r"[A-Za-z_]\w*", " ".join(re.findall(r'"([^"]*)"', code))))
    code_bare = set(re.findall(r"[A-Za-z_]\w*", re.sub(r'"[^"]*"', '""', code)))
    sql_ids = set(re.findall(r"[A-Za-z_]\w*", re.sub(r"'[^']*'", "''", sql)))
    out = []
    for w in sorted(sql_ids & quoted):
        if len(w) < 4 or freq[w] < MIN_FREQ or w.lower() in KEYWORDS or w.startswith("df_") or w.lower() in tables:
            continue
        if w in sql_lits or w in code_bare or re.search(rf"\b{w}\s*\(", sql + code):
            continue
        if len(re.findall(rf"\b{w}\b", sql + code)) != len(re.findall(rf"\b{w}\b", sql + code, flags=re.I)):
            continue
        out.append(w)
    return out


def extend(w: str, rng: random.Random) -> str:
    r = rng.random()
    a, b = rng.sample(EXT, 2)
    if r < 0.5:
        return f"{w}_{a}"
    if r < 0.75:
        return f"{a}_{w}"
    return f"{w}_{a}_{b}"


def stem_pair(rec: dict, rng: random.Random, banned: set[str], freq: Counter, rename_tables: bool):
    """Optional table rename, then 1-2 frequent columns get an extension. Returns (record, table_map, col_map) or None."""
    orig = rec["messages"]
    msgs = orig
    if rename_tables:
        msgs = namecopy.rename(orig, rng, banned)
        if not msgs:
            return None
    olds = re.findall(r"\bdf_(\w+)", orig[2]["content"])
    news = re.findall(r"\bdf_(\w+)", msgs[2]["content"])
    if not olds or len(olds) != len(news):
        return None
    tmap = {}
    for a, b in zip(olds, news):
        if tmap.setdefault(a, b) != b:
            return None
    user, code = msgs[1]["content"], msgs[2]["content"]
    m = re.search(r"```sql\n(.*?)```", user, flags=re.S)
    if not m:
        return None
    cands = stem_candidates(m.group(1), code, {t.lower() for t in news}, freq)
    if not cands:
        return None
    present = namecopy.identifiers(user + " " + code)
    cmap: dict[str, str] = {}
    for c in rng.sample(cands, rng.randint(1, min(2, len(cands)))):
        for _ in range(30):
            y = extend(c, rng)
            if y not in banned and y.lower() not in present and y not in cmap.values():
                cmap[c] = y
                break
    if not cmap:
        return None
    sub = lambda s, mp: [s := re.sub(rf"\b{a}\b", b, s) for a, b in mp.items()][-1]
    u2, c2 = sub(user, cmap), sub(code, cmap)
    rev = {b: a for a, b in cmap.items()}
    if sub(u2, rev) != user or sub(c2, rev) != code:
        return None
    if any(re.search(rf"\b{a}\b", u2 + c2) for a in cmap):    # the short name must be gone
        return None
    out = [dict(x) for x in msgs]
    out[1]["content"], out[2]["content"] = u2, c2
    return {"messages": out}, tmap, cmap


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--spot", type=int, default=200)
    a = ap.parse_args()
    rng = random.Random(SEED)
    banned = namecopy.eval_identifiers()
    mix = lines(MLX / "complex_v17" / "data_mix" / "train.jsonl")
    seen = {sql_of(x) for x in mix} | {sql_of(x) for x in lines(P / "data_q9b" / "train.jsonl")}
    freq = mix_column_freq(mix)
    unseen = lambda d: [x for x in lines(MLX / d / "data" / "train.jsonl") if sql_of(x) not in seen]
    pools = {d: unseen(d) for d in ["classic_v10", "complex_v9", "complex_v16", "targeted_v11", "targeted_v12",
                                    "targeted_v14"]}
    print("unseen pools:", {k: len(v) for k, v in pools.items()})
    taken: set[str] = set()

    def pick(cands: list[str], n: int) -> list[str]:
        c = [x for x in cands if x not in taken]
        out = rng.sample(c, min(n, len(c)))
        taken.update(out)
        return out

    parts: dict[str, list[str]] = {}
    # stem extensions + plain renames from the mix
    order = list(range(len(mix)))
    rng.shuffle(order)
    stems, plain, spot, used, ncols = [], [], [], set(), Counter()
    for i in order:
        if len(stems) >= 3600 and len(plain) >= 1200:
            break
        rec = json.loads(mix[i])
        if len(stems) < 3600:
            r = stem_pair(rec, rng, banned, freq, rename_tables=rng.random() < 0.5)
            if r:
                stems.append(json.dumps(r[0])); used.add(mix[i]); ncols[len(r[2])] += 1
                spot.append((rec, r[0], r[1], r[2]))
                continue
        if len(plain) < 1200:
            r = pm.renamed_pair(rec, rng, banned)
            if r:
                plain.append(json.dumps(r[0])); used.add(mix[i])
                spot.append((rec, r[0], r[1], r[2]))
    parts["stem_ext"], parts["plain_rename"] = stems, plain
    print(f"stem extensions {len(stems)} (columns per pair {dict(ncols)}), plain renames {len(plain)}")

    fam_pool = pools["classic_v10"] + pools["complex_v9"] + pools["complex_v16"]
    parts["lateral"] = pick([x for x in pools["classic_v10"] if RE["lateral"].search(sql_of(x))], 600)
    parts["concat_lit"] = pick([x for x in fam_pool if RE["concat_lit"].search(sql_of(x))], 600)
    parts["interval_join"] = pick([x for x in fam_pool if RE["interval_join"].search(sql_of(x))], 600)
    for d in ["targeted_v11", "targeted_v12", "targeted_v14"]:
        parts[d] = pick(pools[d], 400)
    for d in ["complex_v16", "complex_v9"]:
        parts[d] = pick(pools[d], 600)
    special = set()
    for d in ["complex_v8", "complex_v9", "complex_v16", "targeted_v11", "targeted_v12", "targeted_v13", "targeted_v14"]:
        special |= {sql_of(x) for x in lines(MLX / d / "data" / "train.jsonl")}
    parts["replay"] = rng.sample([x for x in mix if sql_of(x) not in special and x not in used], 3000)
    train = [x for v in parts.values() for x in v]
    rng.shuffle(train)

    mvalid = [json.loads(x) for x in lines(MLX / "complex_v17" / "data_mix" / "valid.jsonl")]
    rng.shuffle(mvalid)
    vstem = []
    for rec in mvalid:
        r = stem_pair(rec, rng, banned, freq, rename_tables=rng.random() < 0.5)
        if r:
            vstem.append(json.dumps(r[0]))
        if len(vstem) == 100:
            break
    valid = lines(P / "data_q9b" / "valid.jsonl") + vstem

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
    (OUT / "samples_stem.jsonl").write_text(
        "".join(json.dumps({"tables": t, "columns": c, "sql": n["messages"][1]["content"],
                            "code": n["messages"][2]["content"]}) + "\n" for _, n, t, c in spot[:12]))
    stats = {k: len(v) for k, v in parts.items()} | {"train": len(train), "valid": len(valid),
                                                     "valid_stem": len(vstem), "eval_sql_inside_train": hits,
                                                     "stem_columns_per_pair": dict(ncols)}
    if a.spot:
        pm.OUT = OUT
        rng.shuffle(spot)
        stats["spot_check"] = pm.spot_check(spot, a.spot)
    (OUT / "stats.json").write_text(json.dumps(stats, indent=2) + "\n")
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
