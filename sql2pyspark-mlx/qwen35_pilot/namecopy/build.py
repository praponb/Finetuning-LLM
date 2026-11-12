"""Name-copy data for Qwen3.5 run 5 (2026-11-10).

Why: Qwen3.5 run 4 (11,600 iterations) misspells unseen table names by blending them with memorized training names
(doctors -> df_drators, donors -> df_donators, finance_deals -> df_finice_deals, rockets -> df_rockers): 13 of its 16
hold-out failures and 19 complex-shift slips (v17-q4: 1). The training mix reuses the same ~260 table names thousands
of times, so the model can recall instead of copy.

What: take verified pairs from the v17 mix and rename every base table consistently in the SQL and the code
(`X` -> `Y`, `df_X` -> `df_Y`) to a fresh random name per pair, drawn from a large generated vocabulary. A consistent
rename keeps a verified pair correct, and the SQL becomes the only source of the name. New names never equal any
identifier of the eval sets (hold-out, v4, real queries, fixed, complex, schema-shift, complex-shift), so the evals stay
unseen.

Output (data_namecopy/): train = N_RENAMED renamed + N_KEEP original v17-mix pairs (shuffled); valid = the v17 valid
set + 100 renamed valid pairs.
Usage (from sql2pyspark-mlx/):  .venv/bin/python qwen35_pilot/namecopy/build.py                       # run 5
                                .venv/bin/python qwen35_pilot/namecopy/build.py --keep all --out data_m6e  # run 6:
  the whole v17 mix (29,008) with 8,000 of its pairs renamed
"""
from __future__ import annotations

import json
import random
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]          # Finetuning-LLM
MIX = ROOT / "sql2pyspark-mlx/complex_v17/data_mix"
OUT = Path(__file__).resolve().parent.parent / "data_namecopy"
N_RENAMED, N_KEEP, N_VALID_RENAMED, SEED = 8000, 8000, 100, 2026   # run 5; run 6: --keep all --out data_m6e; run 8: --seed 2027 --out data_m6g

EVAL_SETS = ["eval/holdout.jsonl", "eval/holdout_v4.jsonl", "eval/real_queries.jsonl", "eval/test_fixed.jsonl",
             "eval/complex_v8_test.jsonl", "sql2pyspark-mlx/schema_v15/data/schema_shift_meta.jsonl",
             "sql2pyspark-mlx/complex_v16/data/complex_shift_meta.jsonl"]
EVAL_PY = ["eval/real_tables.py", "eval/schema_shift_tables.py", "eval/complex_shift_tables.py"]

NOUNS = """
acrobats actuators adapters admirals aerials agents airships alarms albums alloys almanacs amplifiers anchors anglers
antennas anvils apiaries aquifers arbiters arcades archers archives arenas armatures arrays artisans atlases atriums
auctions avatars awnings badges bakeries ballads balloons bandages banners barges barns barrels basins batons
beacons bellows benches berths beverages bicycles billboards binders biomes blenders blimps blueprints boilers
bollards bonfires bookmarks boulders boutiques bracelets breweries brigades brochures buckets buggies bulletins
bunkers buoys cabins cabinets cables cadets calipers canals candles canisters canteens canyons capsules caravans
cargoes carousels cartons cascades casks castings catalysts cathedrals cauldrons cellars ceramics chalets chandlers
channels chapels chariots charters chimneys chisels chronicles cinemas circuits citadels clamps clinics clippers
cloisters coasters cobblers cockpits codices cohorts colonies comets compasses composers condors conduits consoles
convoys coolers corridors cottages couriers coves cranes craters crates creameries crescents crews crossings cruisers
crystals cultivars cupolas currents curtains cutters cyclones dairies dams dashboards debuts decoders delis deltas
depots derricks dioramas dispatches districts docks domes dories drafts dredges drills drones dunes dynamos easels
eclipses editors eddies elevators embassies emitters engines envoys estates estuaries ferries fiddles filaments
fjords flagons fleets floodgates florists flutes forges fossils foundries fountains freighters frescoes funnels
furnaces gables galleons galleries gantries garrisons gazebos geysers girders glaciers gliders goblets gondolas
gorges granaries greenhouses grottos guilds gyroscopes hammocks hangars harbors harvesters hatcheries headlands
helmets hermitages hives holsters hostels hulls igloos inkwells inlets jetties jukeboxes junctions kayaks kettles
kilns kiosks ladders lagoons lanterns lathes lattices ledgers lenses levers libretti lighthouses lockers locomotives
lodges looms lyrics magnets mainframes manors mantles marinas markets masts meadows medallions meteors mills
minarets mines mirrors monoliths mosaics motels murals museums nebulae nozzles oases observatories orchards organs
outposts paddocks pagodas palettes pantries parlors pavilions pedals pendulums periscopes pianos piers pistons
planets plateaus plazas plinths podcasts pontoons portals presses prisms propellers pulleys pumps quarries quills
radars rafters railcars ramparts ranches rapids reactors reefs refineries regattas relays reservoirs ridges rigs
rivets robots rotors saddles satellites sawmills scaffolds scanners schooners scrolls sculptures sensors sextants
shelters shipyards shrines shutters silos skiffs sleds sluices smelters snorkels sonatas spindles spires sprockets
stables stadiums statues steamers stencils studios submarines summits sundials switches tablets tankers tapestries
taverns telescopes terminals terraces textiles theaters thermostats thickets tiaras timbers toolkits torches totems
towers trailers trams trellises tributaries trolleys trophies tugboats tunnels turbines turrets typewriters
valves vaults velodromes vineyards violins volcanoes wagons warehouses waterfalls weathervanes wharves whistles
windmills workshops yachts zeppelins
""".split()
QUALS = """alpha amber arctic azure bronze cedar coastal cobalt copper coral crimson daily delta eastern emerald
global golden granite harbor indigo inland ivory jade lunar maple marble midnight northern ochre onyx pacific pine
polar quartz regional river rural scarlet silver solar southern summit teal topaz urban valley western willow""".split()
PREFIXES = ["dim_", "fact_", "tbl_", "stg_", "raw_", "ref_", "core_"]
SUFFIXES = ["_log", "_master", "_hist", "_daily", "_registry", "_snapshot", "_events", "_v2"]


def identifiers(text: str) -> set[str]:
    return set(re.findall(r"[A-Za-z_][A-Za-z0-9_]*", text.lower()))


def eval_identifiers() -> set[str]:
    ids: set[str] = set()
    for p in EVAL_SETS:
        for line in open(ROOT / p):
            r = json.loads(line)
            ids |= identifiers(r.get("sql", "") + " " + r.get("code", ""))
    for p in EVAL_PY:
        ids |= identifiers((ROOT / p).read_text())
    for p in ["sql2pyspark-mlx/schema_v15/synth_catalog.py", "sql2pyspark-mlx/complex_v16/cx_patch.py"]:
        ids |= identifiers((ROOT / p).read_text())   # incl. the held-out domains' vocabulary
    return ids


def new_name(rng: random.Random) -> str:
    n = rng.choice(NOUNS)
    r = rng.random()
    if r < 0.35:
        return n
    if r < 0.60:
        return f"{rng.choice(QUALS)}_{n}"
    if r < 0.80:
        return f"{rng.choice(PREFIXES)}{n}"
    if r < 0.90:
        return f"{n}{rng.choice(SUFFIXES)}"
    return f"{rng.choice(NOUNS)[:-1]}_{n}"


def tables_of(sql: str, code: str) -> list[str] | None:
    ctes = {m.lower() for m in re.findall(r"\b([A-Za-z_]\w*)\s+AS\s*\(", sql, flags=re.I)}
    refs = {m for m in re.findall(r"\b(?:FROM|JOIN)\s+([A-Za-z_]\w*)", sql, flags=re.I)}
    tabs = sorted(t for t in refs if t.lower() not in ctes and t.upper() not in {"SELECT", "LATERAL", "UNNEST"})
    if not tabs or any(f"df_{t}" not in code for t in tabs):
        return None
    return tabs


def rename(msgs: list[dict], rng: random.Random, banned: set[str]) -> list[dict] | None:
    user, code = msgs[1]["content"], msgs[2]["content"]
    m = re.search(r"```sql\n(.*?)```", user, flags=re.S)
    if not m:
        return None
    tabs = tables_of(m.group(1), code)
    if not tabs:
        return None
    present = identifiers(user + " " + code)
    mapping: dict[str, str] = {}
    for t in tabs:
        for _ in range(50):
            y = new_name(rng)
            if y not in banned and y not in present and y not in mapping.values():
                mapping[t] = y
                break
        else:
            return None
    for t, y in mapping.items():
        code = re.sub(rf"\bdf_{t}\b", f"df_{y}", code)
        code = re.sub(rf"\b{t}\b", y, code)
        user = re.sub(rf"\b{t}\b", y, user)
    for t in mapping:   # nothing of the old base-table names may remain
        if re.search(rf"\b(df_)?{t}\b", user + code):
            return None
    out = [dict(x) for x in msgs]
    out[1]["content"], out[2]["content"] = user, code
    return out


def main() -> None:
    global OUT, N_KEEP
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=None, help="output folder name next to data_namecopy (default data_namecopy)")
    ap.add_argument("--keep", default=str(N_KEEP), help="original pairs to keep: a number or 'all' (the rest of the mix)")
    ap.add_argument("--seed", type=int, default=SEED, help="random seed (run 8: 2027 = fresh renames)")
    a = ap.parse_args()
    if a.out:
        OUT = OUT.parent / a.out
    N_KEEP = 10**9 if a.keep == "all" else int(a.keep)
    rng = random.Random(a.seed)
    banned = eval_identifiers()
    print(f"banned identifiers: {len(banned)}; vocabulary: {len(NOUNS)} nouns")
    clash = [n for n in NOUNS if n in banned]
    print(f"nouns that clash with eval identifiers (skipped when drawn): {len(clash)} {clash[:12]}")
    train = [json.loads(l) for l in open(MIX / "train.jsonl")]
    valid = [json.loads(l) for l in open(MIX / "valid.jsonl")]
    rng.shuffle(train)
    renamed, used = [], set()
    for i, r in enumerate(train):
        if len(renamed) == N_RENAMED:
            break
        msgs = rename(r["messages"], rng, banned)
        if msgs:
            renamed.append({"messages": msgs})
            used.add(i)
    keep = [r for i, r in enumerate(train) if i not in used][:N_KEEP]
    out_train = renamed + keep
    rng.shuffle(out_train)
    vren = [x for x in ({"messages": rename(r["messages"], rng, banned)} for r in valid) if x["messages"]][:N_VALID_RENAMED]
    OUT.mkdir(exist_ok=True)
    with open(OUT / "train.jsonl", "w") as f:
        f.writelines(json.dumps(r) + "\n" for r in out_train)
    with open(OUT / "valid.jsonl", "w") as f:
        f.writelines(json.dumps(r) + "\n" for r in valid + vren)
    names = {n for r in renamed for n in re.findall(r"\bdf_(\w+)", r["messages"][2]["content"])}
    print(f"train {len(out_train)} ({len(renamed)} renamed, {len(keep)} original), valid {len(valid) + len(vren)} "
          f"({len(vren)} renamed); distinct new table names in train: {len(names)}")


if __name__ == "__main__":
    main()
