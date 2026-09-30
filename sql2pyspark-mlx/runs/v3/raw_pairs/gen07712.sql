SELECT id, device, started_at, RANK() OVER (PARTITION BY device ORDER BY started_at) AS started_at_rank FROM sessions;
