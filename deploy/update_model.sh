#!/bin/zsh
# Put the last export (sql2pyspark.gguf on the Mac) on the Ubuntu server as sql2pyspark:<TAG> and make it the server's
# sql2pyspark:latest, keeping the old latest as sql2pyspark:<PREV> for rollback. No sudo needed (Ollama CLI + scp).
#   zsh deploy/update_model.sh v8 v7          # after `make promote TAG=v8 PREV=v7` on the Mac
# Rollback on the server:  ssh prapon@192.168.1.49 'ollama cp sql2pyspark:v7 sql2pyspark:latest'
set -e
TAG=${1:?usage: update_model.sh TAG PREV}
PREV=${2:?usage: update_model.sh TAG PREV}
SERVER=${SERVER:-prapon@192.168.1.49}
cd "$(dirname "$0")/.."
echo "[$(date +%T)] copying sql2pyspark.gguf ($(du -h sql2pyspark.gguf | cut -f1)) to $SERVER"
scp -q sql2pyspark.gguf "$SERVER":~/sql2pyspark.gguf.new
ssh "$SERVER" "set -e
  ollama show sql2pyspark:$PREV >/dev/null 2>&1 || ollama cp sql2pyspark:latest sql2pyspark:$PREV
  mv ~/sql2pyspark.gguf.new ~/sql2pyspark.gguf
  cd ~ && ollama create sql2pyspark:$TAG -f Modelfile
  ollama cp sql2pyspark:$TAG sql2pyspark:latest
  ollama list | grep sql2pyspark"
echo "[$(date +%T)] done; local ID for comparison:"; ollama list | grep "sql2pyspark:$TAG "
