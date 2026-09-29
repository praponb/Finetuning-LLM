#!/bin/bash
# sql2pyspark server setup (run once with sudo; copy of ~/setup_sql2pyspark.sh on the server): Java for the Kimi K3 checks, run the app from the repo,
# keep the Kimi repair OFF (empty key) until a login protects the public site.
set -euo pipefail
UNIT=/etc/systemd/system/sql2pyspark.service
REPO=/home/prapon/Finetuning-LLM
[ "$(id -u)" -eq 0 ] || { echo "run with: sudo bash $0"; exit 1; }
[ -f "$UNIT" ] || { echo "not found: $UNIT"; exit 1; }
[ -f "$REPO/app.py" ] && [ -f "$REPO/kimi_repair.py" ] || { echo "repo files missing in $REPO"; exit 1; }
echo "== 1/4 install Java 17"
apt-get update -qq && apt-get install -y -qq openjdk-17-jre-headless >/dev/null
java -version 2>&1 | head -1
echo "== 2/4 back up and update $UNIT"
cp "$UNIT" "$UNIT.bak-$(date +%Y%m%d-%H%M%S)"
sed -i "s#^WorkingDirectory=/home/prapon\$#WorkingDirectory=$REPO#" "$UNIT"
grep -q "^Environment=KIMI_API_KEY=" "$UNIT" || sed -i "/^Environment=PYTHONUNBUFFERED=1/a Environment=KIMI_API_KEY=" "$UNIT"
grep -E "^(WorkingDirectory|ExecStart|Environment)=" "$UNIT"
echo "== 3/4 restart the app"
systemctl daemon-reload
systemctl restart sql2pyspark
sleep 8
echo "== 4/4 check"
systemctl is-active sql2pyspark
curl -s -m 10 http://127.0.0.1:8501/_stcore/health; echo
echo "done. Kimi repair stays OFF until you remove the line \"Environment=KIMI_API_KEY=\" from $UNIT"
