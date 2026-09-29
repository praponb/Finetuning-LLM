SELECT s.browser, a.page, COUNT(*) AS rows_total, AVG(a.duration_sec) AS avg_duration_sec FROM web_events a JOIN sessions s ON a.session_id = s.id GROUP BY s.browser, a.page ORDER BY avg_duration_sec
