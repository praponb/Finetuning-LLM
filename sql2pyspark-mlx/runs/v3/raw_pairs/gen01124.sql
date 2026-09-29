WITH ranked_sessions AS (SELECT id, country, started_at, customer_id, RANK() OVER (PARTITION BY country ORDER BY started_at DESC) rn FROM sessions) SELECT * FROM ranked_sessions WHERE rn = 1
