WITH top_sessions AS (SELECT *, RANK() OVER (PARTITION BY browser ORDER BY started_at DESC) AS row_num FROM sessions) SELECT id, browser, started_at FROM top_sessions WHERE row_num <= 1
