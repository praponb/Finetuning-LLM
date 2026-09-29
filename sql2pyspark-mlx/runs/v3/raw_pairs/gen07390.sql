SELECT id, browser, started_at, LEAD(started_at, 2) OVER (PARTITION BY browser ORDER BY started_at, id) AS next_started_at FROM sessions WHERE device NOT IN ('mobile', 'tablet', 'desktop')
