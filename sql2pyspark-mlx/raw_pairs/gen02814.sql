SELECT id, started_at, country, LAG(started_at, 1) OVER (PARTITION BY MONTH(started_at), country ORDER BY started_at, id) AS prev_started_at FROM sessions
