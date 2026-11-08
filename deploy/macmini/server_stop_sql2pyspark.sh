#!/bin/bash
# Run ON THE UBUNTU SERVER once, after sql2pyspark.praponb.com moved to the Mac mini (2026-11-08):
#   sudo bash ~/server_stop_sql2pyspark.sh
# Stops the server's Streamlit app and removes its rule from the inventory-tunnel config (backup kept).
# The server's Ollama and its sql2pyspark:* tags stay, for rollback (see deploy/macmini/README.md).
set -euo pipefail
[[ $EUID -eq 0 ]] || { echo "run with sudo"; exit 1; }
CFG=/etc/cloudflared/config.yml
BAK=$CFG.bak-20261108-sql2pyspark
NEW=$(mktemp)

[[ -f $BAK ]] || cp -a $CFG $BAK
# drop the comment, hostname and service lines of the sql2pyspark rule and the blank line after it
awk '
  /# SQL to PySpark/ {skip=1; next}
  skip && /hostname: sql2pyspark.praponb.com/ {next}
  skip && /service: http:\/\/localhost:8501/ {skip=2; next}
  skip==2 && /^[[:space:]]*$/ {skip=0; next}
  {skip=(skip==2?0:skip); print}
' $CFG > $NEW
grep -q sql2pyspark $NEW && { echo "rule still present, nothing changed"; exit 1; }
cloudflared tunnel --config $NEW ingress validate
diff $CFG $NEW || true
install -m 644 $NEW $CFG && rm -f $NEW

systemctl disable --now sql2pyspark
systemctl restart cloudflared
sleep 5
echo "sql2pyspark: $(systemctl is-active sql2pyspark || true)   cloudflared: $(systemctl is-active cloudflared)"
echo "backup: $BAK"
