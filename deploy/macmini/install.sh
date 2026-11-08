#!/bin/zsh
# Install (or reinstall) the three LaunchDaemons that serve sql2pyspark.praponb.com from the Mac mini.
#   sudo zsh deploy/macmini/install.sh
# Before: quit the Ollama app and remove it from System Settings -> General -> Login Items
# (its own `ollama serve` would compete with com.praponb.ollama for port 11434).
set -e
[[ $EUID -eq 0 ]] || { echo "run with sudo"; exit 1; }
cd "$(dirname "$0")"
LABELS=(com.praponb.ollama com.praponb.sql2pyspark com.praponb.cloudflared-sql2pyspark)

[[ -f /Users/praponb/.cloudflared/sql2pyspark-config.yml ]] || { echo "missing ~/.cloudflared/sql2pyspark-config.yml"; exit 1; }
if pgrep -x ollama >/dev/null && ! launchctl print system/com.praponb.ollama >/dev/null 2>&1; then
  echo "Ollama (the app) is still running: quit it from the menu bar first"; exit 1
fi
sudo -u praponb mkdir -p /Users/praponb/Library/Logs/sql2pyspark

for L in $LABELS; do
  launchctl bootout system/$L 2>/dev/null || true
  install -m 644 -o root -g wheel $L.plist /Library/LaunchDaemons/$L.plist
  plutil -lint /Library/LaunchDaemons/$L.plist >/dev/null
  launchctl bootstrap system /Library/LaunchDaemons/$L.plist
  echo "loaded $L"
done

pmset -a sleep 0 autorestart 1          # stay awake; boot again after a power cut (FileVault still asks at the Mac)

echo "waiting for Ollama and the app ..."
for i in {1..60}; do curl -sf http://127.0.0.1:11434/api/tags >/dev/null && break; sleep 1; done
for i in {1..60}; do curl -sf http://127.0.0.1:8502/_stcore/health >/dev/null && break; sleep 1; done
echo "ollama: $(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:11434/api/tags)  app: $(curl -s http://127.0.0.1:8502/_stcore/health)"
for L in $LABELS; do printf '%-40s %s\n' $L "$(launchctl print system/$L | awk '/^\tstate =/{print $3}')"; done
