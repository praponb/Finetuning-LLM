#!/bin/sh
# Start Ollama and register the fine-tuned model from the mounted GGUF (once; kept in the volume).
set -e
ollama serve &
SERVER=$!
until ollama list >/dev/null 2>&1; do sleep 1; done
if ! ollama list | grep -q "^sql2pyspark:latest"; then
    echo "registering sql2pyspark from /models/sql2pyspark.gguf ..."
    cd /models && ollama create sql2pyspark -f Modelfile
fi
wait $SERVER
