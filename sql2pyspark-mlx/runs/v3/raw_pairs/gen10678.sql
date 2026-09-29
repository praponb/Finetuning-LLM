SELECT id, customer_id, country, started_at, NTILE(10) OVER (PARTITION BY customer_id, country ORDER BY started_at, id) AS started_at_ntile FROM sessions
