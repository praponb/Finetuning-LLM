# Export helpers for model variants (11-23 improvement plan, steps A / B). Sourced by the step scripts, from the repo root.
# Same export path as chain_q9e.sh: fuse --dequantize -> fix_norms -> f16 GGUF (--no-mtp) -> llama-quantize -> Ollama.
# Never touches sql2pyspark:latest / v18a-q4 / v15-q4, the root GGUF files or the site.
export PATH=/opt/homebrew/opt/openjdk@17/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin JAVA_HOME=/opt/homebrew/opt/openjdk@17
export HF_HUB_OFFLINE=1
M=sql2pyspark-mlx; P=$M/qwen35_pilot; V=$P/variants; PY=$M/.venv/bin/python; BASE=mlx-community/Qwen3.5-9B-4bit
G=$V/gguf; W=$V/work; R=eval/results/variants
mkdir -p $G $W $R
step() { echo "[$(date +%H:%M:%S)] $*"; }
unload() { for m in $(ollama ps 2>/dev/null | awk 'NR>1{print $1}'); do ollama stop $m; done }

# fuse_adapter <adapter dir, relative to sql2pyspark-mlx> <out hf dir>   (skips if done)
fuse_adapter() {
  [ -s $2/.fused_ok ] && return 0
  rm -rf $2
  (cd $M && .venv/bin/mlx_lm.fuse --model $BASE --adapter-path $1 --save-path ${2#$M/} --dequantize) || return 1
  $PY $P/fix_norms.py $2 || return 1
  touch $2/.fused_ok
}
# fuse_ckpt <adapter dir> <checkpoint file name inside it> <out hf dir>: fuse one checkpoint file
fuse_ckpt() {
  local tmp=$W/ad_$(basename $2 .safetensors)
  rm -rf $tmp; mkdir -p $tmp
  cp $M/$1/adapter_config.json $tmp/; ln -sf $PWD/$M/$1/$2 $tmp/adapters.safetensors
  fuse_adapter ${tmp#$M/} $3; local rc=$?
  rm -rf $tmp; return $rc
}
# to_f16 <hf dir> <out f16 gguf>
to_f16() { [ -s $2 ] || $PY llama.cpp/convert_hf_to_gguf.py $1 --outtype f16 --no-mtp --outfile $2; }
# quant <f16 gguf> <out gguf> <type> [imatrix file]
quant() {
  [ -s $2 ] && return 0
  if [ -n "$4" ]; then llama.cpp/build/bin/llama-quantize --imatrix $4 $1 $2 $3 > $2.log 2>&1
  else llama.cpp/build/bin/llama-quantize $1 $2 $3 > $2.log 2>&1; fi
}
# tag <gguf> <tag suffix, e.g. q9e-q8>
tag() {
  ollama list | grep -q "^sql2pyspark:$2 " && return 0
  sed "s#^FROM .*#FROM $PWD/$1#" $P/Modelfile.q35 > $V/Modelfile.$2
  ollama create sql2pyspark:$2 -f $V/Modelfile.$2
}
# smoke <tag>: 3 conversions, fail on a <think> leak or a reply without a python block
smoke() {
  $PY - "$1" <<'EOF'
import sys, requests
bad = 0
for sql in ["SELECT id, name FROM customers WHERE city = 'Bangkok';",
            "SELECT city, COUNT(*) AS n FROM customers GROUP BY city HAVING COUNT(*) > 1;",
            "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id ORDER BY o.id;"]:
    r = requests.post("http://localhost:11434/api/chat", json={"model": f"sql2pyspark:{sys.argv[1]}", "stream": False,
        "think": False, "options": {"temperature": 0, "seed": 0, "num_predict": 1024},
        "messages": [{"role": "user", "content": f"Convert SQL to PySpark:\n```sql\n{sql}\n```"}]}, timeout=600).json()
    t = r["message"]["content"]
    bad += ("<think>" in t) or ("```python" not in t)
print(f"smoke {sys.argv[1]}: {3 - bad}/3 ok"); sys.exit(1 if bad else 0)
EOF
}
# quick_evals <tag>: hold-out, v4, fixed (+ dev set if it exists), same flags as the chain, results in eval/results/variants
quick_evals() {
  local E=( $PY eval/evaluate.py --model sql2pyspark:$1 --think false --timeout 600 )
  for s in holdout holdout_v4 test_fixed; do
    [ -s $R/$1__$s.json ] || { step "$1 $s"; "${E[@]}" --set eval/$s.jsonl --out $R/$1__$s.json > $R/$1__$s.log 2>&1; }
  done
  dev_eval $1
  unload
}
# dev_eval <tag>: the dev set only (if it exists)
dev_eval() {
  [ -s eval/dev_short.jsonl ] && [ ! -s $R/$1__dev_short.json ] || return 0
  step "$1 dev_short"
  local th=(--think false); [[ $1 == q9* ]] || th=()      # v18a (Qwen2.5) was always scored without the think flag
  $PY eval/evaluate.py --model sql2pyspark:$1 "${th[@]}" --timeout 600 --set eval/dev_short.jsonl \
    --out $R/$1__dev_short.json > $R/$1__dev_short.log 2>&1
}
