"""v16 pairs: the complex_v8 families (multi-stage pipelines, many-window queries, set operations, gaps-and-islands,
FULL OUTER joins, ROLLUP / CUBE / GROUPING SETS, PIVOT / UNPIVOT) on the synthetic schema_v15 catalog, in the mirror
window style, so the complex forms come with names the model has never seen (the v15 lesson).

`interval` is left out (it needs complex_v8's own coverage / tariff / ticket tables); `sessionize` picks a synthetic
table with a timestamp and a key instead of complex_v8's fixed list. Pipelines get 3-7 stages as in complex_v8, plus
a share of longer ones (6-9 stages) towards the long real-world queries.

  python generate.py --split train   --out raw_v16          (the 30 training domains)
  python generate.py --split heldout --out raw_v16_shift    (the 5 schema_v15 HELDOUT_DOMAINS: complex schema-shift set)
"""
from __future__ import annotations

import argparse
import json
import random
import re
import sys
from collections import Counter
from pathlib import Path

import cx_patch

FAMILIES = {"pipeline": 30, "long_pipeline": 10, "window_heavy": 18, "setop": 14, "sessionize": 8, "full_outer": 5,
            "grouping": 10, "reshape": 8}
COUNTS = {"train": 13500, "heldout": 450}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--split", choices=["train", "heldout"], default="train")
    ap.add_argument("--out", default="raw_v16")
    ap.add_argument("--seed", type=int, default=16)
    ap.add_argument("--n", type=int, default=0, help="number of pairs (default: COUNTS[split])")
    args = ap.parse_args()
    held = args.split == "heldout"
    cx_patch.install(held)
    import generate as gen          # complex_v8/generate.py, now on the synthetic tables
    from catalog import TABLES
    cx_patch.rebind(gen)
    gen.WINDOW_STYLE = "mirror"
    gen.FIX_UNION_ALIASES = False

    ts_tables = [t for t in TABLES if any(c.kind == "ts" for c in t.columns)
                 and any(c.kind in ("fk", "cat") and not c.nullable for c in t.columns)]

    def build(rng, fam):
        if fam == "sessionize":
            q = gen.Q(rng)
            cur = gen.st_source(q, rng.choice(ts_tables), n_derive=(0, 1), n_pred=(0, 1))
            cur = gen.st_sessionize(q, cur) or cur
            cur = gen.middle(q, cur, rng.randint(0, 2))
            res = gen.final(q, cur)
            if res is None:
                return None
            sql, code = q.render(*res)
            return sql, code, set(q.features) | {"sessionize_synth"}, q.order_cols
        if fam == "long_pipeline":
            q = gen.Q(rng)
            cur = gen.st_source(q, gen.pick_table(rng))
            cur = gen.middle(q, cur, rng.randint(5, 8))
            res = gen.final(q, cur)
            if res is None:
                return None
            sql, code = q.render(*res)
            return sql, code, set(q.features) | {"long_pipeline"}, q.order_cols
        return gen.build(rng, fam)

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    rng = random.Random(args.seed + (1 if held else 0))
    n = args.n or COUNTS[args.split]
    fams = list(FAMILIES)
    seen, metas, errs = set(), [], Counter()
    while len(metas) < n:
        fam = rng.choices(fams, [FAMILIES[f] for f in fams])[0]
        sub = random.Random(rng.random())
        try:
            res = build(sub, fam)
        except Exception as e:  # noqa: BLE001  (generator limits on these tables -> skip, counted)
            errs[f"{fam}:{type(e).__name__}"] += 1
            continue
        if not res:
            errs[f"{fam}:none"] += 1
            continue
        sql, code, feats, order_cols = res
        norm = re.sub(r"\s+", " ", sql.lower())
        if norm in seen:
            continue
        seen.add(norm)
        pid = f"x16{len(metas) + 1:05d}"
        (out / f"{pid}.sql").write_text(sql + "\n", encoding="utf-8")
        (out / f"{pid}.py").write_text(code, encoding="utf-8")
        metas.append({"id": pid, "family": f"cx_{fam}", "features": sorted(set(feats) | {"complex_v16"}),
                      "order_cols": order_cols})
    with open(out / "meta_complex.jsonl", "w", encoding="utf-8") as f:
        for m in metas:
            f.write(json.dumps(m) + "\n")
    print(json.dumps({"written": len(metas), "skipped": dict(errs.most_common()),
                      "families": dict(Counter(m["family"] for m in metas))}, indent=2))


if __name__ == "__main__":
    main()
