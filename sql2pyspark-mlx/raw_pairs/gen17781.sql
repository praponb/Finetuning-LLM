SELECT id, device, started_at, RANK() OVER (PARTITION BY device ORDER BY started_at DESC) AS rnk FROM sessions WHERE customer_id = 4
