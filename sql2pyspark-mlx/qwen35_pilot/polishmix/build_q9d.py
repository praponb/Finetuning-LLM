"""Qwen3.5-9B round q9d data (data_q9d), on top of the q9c adapter (owner 2026-11-21: "choice #3", one more round).

q9c-q4 is the best model overall (complex 96, complex-shift 133, 35 name slips vs v18a 53) but is 1 below v18a on
hold-out (60 vs 61) and v4 (19 vs 20). Flips vs v18a (greedy decoding): hold-out 025 loops result_2..result_30 on a
short NOT EXISTS; v4 025 writes "id".alias(...) after LATERAL VIEW EXPLODE; v4 010 puts a two-INTERVAL join on one line
and loses a bracket; v11/v12/v14 dropped vs q9b (q9c had 400 pairs each); one complex-shift over-extension
is_certified_used_used_label (q9c's stem data left derived names like X_label alone when X became X_used).
Train (seed 2031, shuffled; pools exclude the v18a mix, data_q9b AND data_q9c):
  2,600 correlated subqueries (targeted v11 1,000 / v12 800 / v14 800), 600 short anti/semi joins (SQL < 250 chars),
  600 EXPLODE / LATERAL VIEW, 600 multi-condition joins (INTERVAL joins first, then ON ... AND ...) with new table +
  column names, 1,000 stem extensions whose stem has no derived names, 600 stem extensions where derived names follow
  (X -> X_w and X_label -> X_w_label; <= 4 derived names; no EXPLODE pairs), 800 complex (v16 / v9 400 each), 3,200 v18a-mix replay.
Valid: data_q9c/valid.jsonl unchanged (737).
  .venv/bin/python qwen35_pilot/polishmix/build_q9d.py            (from sql2pyspark-mlx)
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
_spec = importlib.util.spec_from_file_location("polishmix_q9c", HERE / "build_q9c.py")
q9c = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(q9c)         # build_q9c.py: stem_pair, stem_candidates, extend; q9c.pm = polishmix/build.py
pm, namecopy = q9c.pm, q9c.namecopy
MLX, P = pm.MLX, pm.P
OUT = P / "data_q9d"
SEED = 2031
MAX_DERIVED = 4
lines, sql_of = pm.lines, pm.sql_of
RE = {"anti": re.compile(r"\bEXISTS\b|NOT\s+IN\s*\(\s*SELECT", re.I),
      "explode": re.compile(r"EXPLODE|LATERAL\s+VIEW", re.I),
      "interval_join": q9c.RE["interval_join"],
      "multi_on": re.compile(r"\bON\b(?:(?!\bWHERE\b|\bJOIN\b|\bGROUP\b|\bORDER\b|\bSELECT\b).)*?\bAND\b", re.I | re.S),
      "join_multiline": re.compile(r"\.join\(\s*\n")}


def token_hit(c: str, ident: str) -> bool:
    """c appears in ident as a whole underscore-delimited part (X in avg_X, X_label, a_X_b), ident != c."""
    return ident != c and re.fullmatch(rf"(?:\w+_)?{re.escape(c)}(?:_\w+)?", ident) is not None


def safe_name(w: str, sql: str, code: str, tables: set[str]) -> bool:
    """q9b/q9c safety rules for a name we rename: in SQL and quoted in code, not in a SQL literal, not a bare code
    name, not a function call, same casing everywhere, not a table."""
    sql_lits = set(re.findall(r"[A-Za-z_]\w*", " ".join(re.findall(r"'[^']*'", sql))))
    quoted = set(re.findall(r"[A-Za-z_]\w*", " ".join(re.findall(r'"([^"]*)"', code))))
    code_bare = set(re.findall(r"[A-Za-z_]\w*", re.sub(r'"[^"]*"', '""', code)))
    sql_ids = set(re.findall(r"[A-Za-z_]\w*", re.sub(r"'[^']*'", "''", sql)))
    if w not in sql_ids or w not in quoted or w in sql_lits or w in code_bare or w.lower() in tables:
        return False
    if w.lower() in q9c.KEYWORDS or w.startswith("df_") or re.search(rf"\b{w}\s*\(", sql + code):
        return False
    return len(re.findall(rf"\b{w}\b", sql + code)) == len(re.findall(rf"\b{w}\b", sql + code, flags=re.I))


def _subst(s: str, mp: dict[str, str]) -> str:
    for a, b in mp.items():
        s = re.sub(rf"\b{re.escape(a)}\b", b, s)
    return s


def stem_clean(rec: dict, rng: random.Random, banned: set[str], freq: Counter):
    """q9c stem extension, kept only if no other identifier in the pair still contains the old stem as a part."""
    r = q9c.stem_pair(rec, rng, banned, freq, rename_tables=rng.random() < 0.5)
    if not r:
        return None
    new, _, cmap = r
    ids = set(re.findall(r"[A-Za-z_]\w*", new["messages"][1]["content"] + " " + new["messages"][2]["content"]))
    if any(token_hit(c, i) for c in cmap for i in ids - set(cmap.values())):
        return None
    return r


def stem_derived(rec: dict, rng: random.Random, banned: set[str], freq: Counter):
    """Stem X (mix freq >= MIN_FREQ) that has derived names in the pair: X -> X_w AND every derived name follows
    (X_label -> X_w_label, avg_X -> avg_X_w). Tables renamed in half. Returns (record, table_map, col_map) or None."""
    orig = rec["messages"]
    msgs = orig
    if rng.random() < 0.5:
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
    sql = m.group(1)
    tables = {t.lower() for t in news}
    ids = set(re.findall(r"[A-Za-z_]\w*", user + " " + code))
    cands = []
    for c in q9c.stem_candidates(sql, code, tables, freq):
        derived = sorted(i for i in ids if token_hit(c, i))
        if 1 <= len(derived) <= MAX_DERIVED and all(safe_name(d, sql, code, tables) for d in derived):
            cands.append((c, derived))
    if not cands:
        return None
    c, derived = rng.choice(cands)
    present = namecopy.identifiers(user + " " + code)
    for _ in range(30):
        y = q9c.extend(c, rng)
        cmap = {c: y} | {d: re.sub(rf"(?<![A-Za-z0-9]){re.escape(c)}(?![A-Za-z0-9])", y, d, count=1) for d in derived}
        news_ = list(cmap.values())
        if len(set(news_)) == len(news_) and not any(v in banned or v.lower() in present for v in news_):
            break
    else:
        return None
    u2, c2 = _subst(user, cmap), _subst(code, cmap)
    rev = {b: a for a, b in cmap.items()}
    if _subst(u2, rev) != user or _subst(c2, rev) != code:
        return None
    if any(re.search(rf"\b{re.escape(a)}\b", u2 + c2) for a in cmap):     # old names must be gone
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
    q9b_tr, q9c_tr = lines(P / "data_q9b" / "train.jsonl"), lines(P / "data_q9c" / "train.jsonl")
    seen = {sql_of(x) for x in mix + q9b_tr + q9c_tr}
    freq = q9c.mix_column_freq(mix)
    srcs = ["classic_v10", "complex_v9", "complex_v16", "targeted_v11", "targeted_v12", "targeted_v14"]
    pools = {d: [x for x in lines(MLX / d / "data" / "train.jsonl") if sql_of(x) not in seen] for d in srcs}
    print("unseen pools:", {k: len(v) for k, v in pools.items()})
    taken: set[str] = set()

    def pick(cands: list[str], n: int) -> list[str]:
        c = [x for x in cands if x not in taken]
        out = rng.sample(c, min(n, len(c)))
        taken.update(out)
        return out

    parts: dict[str, list[str]] = {}
    for d, n in [("targeted_v11", 1000), ("targeted_v12", 800), ("targeted_v14", 800)]:
        parts[d] = pick(pools[d], n)
    allp = [x for d in srcs for x in pools[d]]
    parts["short_anti"] = pick([x for x in allp if RE["anti"].search(sql_of(x)) and len(sql_of(x)) < 250], 600)
    parts["explode"] = pick([x for x in allp if RE["explode"].search(sql_of(x))], 600)
    for d in ["complex_v16", "complex_v9"]:
        parts[d] = pick(pools[d], 400)

    # renamed parts from already-seen pairs (mix first: the v18a layout)
    special = set()
    for d in ["complex_v8", "complex_v9", "complex_v16", "targeted_v11", "targeted_v12", "targeted_v13", "targeted_v14"]:
        special |= {sql_of(x) for x in lines(MLX / d / "data" / "train.jsonl")}
    used: set[str] = set()
    spot: list[tuple] = []
    code_of = lambda x: json.loads(x)["messages"][2]["content"]
    seen_all = list(dict.fromkeys(mix + q9b_tr + q9c_tr))
    ivl = [x for x in seen_all if RE["interval_join"].search(sql_of(x))]
    multi = [x for x in seen_all if x not in set(ivl) and RE["multi_on"].search(sql_of(x))
             and RE["join_multiline"].search(code_of(x))]
    rng.shuffle(ivl); rng.shuffle(multi)
    mj, n_ivl = [], 0
    for x in ivl + multi:
        if len(mj) >= 600:
            break
        rec = json.loads(x)
        r = pm.renamed_pair(rec, rng, banned)
        if r:
            mj.append(json.dumps(r[0])); used.add(x); spot.append((rec, r[0], r[1], r[2]))
            n_ivl += x in ivl
    parts["multi_join_renamed"] = mj
    print(f"multi-condition joins renamed {len(mj)} ({n_ivl} INTERVAL; pools {len(ivl)} INTERVAL, {len(multi)} other)")

    order = list(range(len(mix)))
    rng.shuffle(order)
    clean, derived, ders = [], [], Counter()
    for i in order:
        if len(clean) >= 1000 and len(derived) >= 600:
            break
        if mix[i] in used:
            continue
        if RE["explode"].search(sql_of(mix[i])):     # nested struct fields: the Spark spot-check can't rename them
            continue
        rec = json.loads(mix[i])
        if len(derived) < 600:
            r = stem_derived(rec, rng, banned, freq)
            if r:
                derived.append(json.dumps(r[0])); used.add(mix[i]); spot.append((rec, r[0], r[1], r[2]))
                ders[len(r[2]) - 1] += 1
                continue
        if len(clean) < 1000:
            r = stem_clean(rec, rng, banned, freq)
            if r:
                clean.append(json.dumps(r[0])); used.add(mix[i]); spot.append((rec, r[0], r[1], r[2]))
    parts["stem_clean"], parts["stem_derived"] = clean, derived
    print(f"stem clean {len(clean)}, stem derived {len(derived)} (derived names per pair {dict(ders)})")

    parts["replay"] = rng.sample([x for x in mix if sql_of(x) not in special and x not in used], 3200)
    train = [x for v in parts.values() for x in v]
    rng.shuffle(train)
    valid = lines(P / "data_q9c" / "valid.jsonl")

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
    samples = [s for s in spot if len(s[3]) > 1 and any(token_hit(k, j) for k in s[3] for j in s[3])][:6]
    samples += [s for s in spot if RE["interval_join"].search(s[1]["messages"][1]["content"])][:6]
    (OUT / "samples.jsonl").write_text(
        "".join(json.dumps({"tables": t, "columns": c, "sql": n["messages"][1]["content"],
                            "code": n["messages"][2]["content"]}) + "\n" for _, n, t, c in samples))
    stats = {k: len(v) for k, v in parts.items()} | {"train": len(train), "valid": len(valid),
                                                     "eval_sql_inside_train": hits,
                                                     "multi_join_interval": n_ivl,
                                                     "derived_names_per_pair": dict(ders)}
    if a.spot:
        pm.OUT = OUT
        rng.shuffle(spot)
        stats["spot_check"] = pm.spot_check(spot, a.spot)
    (OUT / "stats.json").write_text(json.dumps(stats, indent=2) + "\n")
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
