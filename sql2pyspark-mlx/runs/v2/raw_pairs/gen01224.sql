SELECT id, device, started_at, ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC, id) rn FROM sessions WHERE TO_DATE(started_at) >= '2024-02-01' ORDER BY started_at
