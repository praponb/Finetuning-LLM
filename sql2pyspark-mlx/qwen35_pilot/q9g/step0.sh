#!/bin/zsh
# q9g step 0: is the rank-96 combined adapter exactly q9df3?  A: fused tensors vs avg_weights of fused q9d/q9f/q9f2.
# B: export q9g0-q4 -> same SHA as q9df3-q4.gguf, else hold-out / v4 / fixed must be 61 / 21 / 147.
cd /Users/praponb/GitHub/Finetuning-LLM
source sql2pyspark-mlx/qwen35_pilot/variants/export_lib.sh
caffeinate -s -i -w $$ &
Q=$P/q9g
step "fuse q9d, q9f, q9f2 + the combined adapter"
fuse_adapter qwen35_pilot/adapters_q9d $W/hf_q9d || exit 1
fuse_adapter qwen35_pilot/adapters_q9f $W/hf_q9f || exit 1
fuse_adapter qwen35_pilot/adapters_q9f2 $W/hf_q9f2 || exit 1
fuse_adapter qwen35_pilot/adapters_q9g_init $W/hf_q9g0 || exit 1
[ -d $W/hf_q9df3 ] || $PY $V/avg_weights.py $W/hf_q9df3 $W/hf_q9d $W/hf_q9f $W/hf_q9f2 --weights 0.5 0.25 0.25 || exit 1
step "check A"
$PY $Q/compare_fused.py $W/hf_q9df3 $W/hf_q9g0
step "export q9g0-q4"
to_f16 $W/hf_q9g0 $W/q9g0-f16.gguf || exit 1
quant $W/q9g0-f16.gguf $G/q9g0-q4.gguf Q4_K_M || exit 1
rm -f $W/q9g0-f16.gguf
a=$(shasum -a 256 $G/q9g0-q4.gguf | cut -c1-16); b=$(shasum -a 256 $G/q9df3-q4.gguf | cut -c1-16)
step "check B sha: q9g0-q4 $a vs q9df3-q4 $b"
if [ "$a" = "$b" ]; then step "CHECK B PASS (identical GGUF)"; else
  tag $G/q9g0-q4.gguf q9g0-q4 || exit 1
  smoke q9g0-q4
  for s in holdout holdout_v4 test_fixed; do
    [ -s $R/q9g0-q4__$s.json ] || { step "q9g0-q4 $s"; $PY eval/evaluate.py --model sql2pyspark:q9g0-q4 --think false --timeout 600 --set eval/$s.jsonl --out $R/q9g0-q4__$s.json > $R/q9g0-q4__$s.log 2>&1; }
  done
  unload
  $PY - <<'PY'
import json
def sc(t, s):
    d = json.load(open(f"eval/results/variants/{t}__{s}.json"))["summary"]["ALL"]; return round(d["strict_match"] * d["n"])
r = {s: (sc("q9g0-q4", s), sc("q9df3-q4", s)) for s in ("holdout", "holdout_v4", "test_fixed")}
print("SCORES q9g0 vs q9df3:", r)
print("CHECK B", "PASS" if all(a >= b for a, b in r.values()) else "FAIL")
PY
fi
step "STEP0 DONE"
