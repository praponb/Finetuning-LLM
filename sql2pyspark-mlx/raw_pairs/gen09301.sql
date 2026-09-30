SELECT id, device, started_at, NTILE(2) OVER (PARTITION BY device ORDER BY started_at, id) AS quartile FROM sessions;
