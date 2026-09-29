SELECT id, device, started_at, DENSE_RANK() OVER (PARTITION BY device ORDER BY started_at) AS dense_rnk FROM sessions
