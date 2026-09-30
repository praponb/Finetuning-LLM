SELECT id, started_at, LAG(started_at, 1) OVER (ORDER BY started_at, id) AS prev_started_at FROM sessions;
