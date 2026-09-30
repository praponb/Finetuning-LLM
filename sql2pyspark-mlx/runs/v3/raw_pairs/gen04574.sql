SELECT id, device, started_at, ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at, id) rn FROM sessions;
