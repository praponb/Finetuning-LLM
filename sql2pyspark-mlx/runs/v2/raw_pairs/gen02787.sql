SELECT id, browser, started_at, LAG(started_at, 2) OVER (PARTITION BY browser ORDER BY started_at, id) AS prev_started_at FROM sessions WHERE browser IN ('Safari', 'Chrome', 'Firefox')
