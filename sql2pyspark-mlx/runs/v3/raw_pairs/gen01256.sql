SELECT id, country, started_at, LEAD(started_at, 1) OVER (PARTITION BY country ORDER BY started_at, id) AS next_started_at FROM sessions ORDER BY next_started_at DESC
