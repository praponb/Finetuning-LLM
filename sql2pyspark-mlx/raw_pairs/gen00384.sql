SELECT s.browser, ROUND(AVG(we.duration_sec), 2) avg_duration_sec FROM web_events we LEFT JOIN sessions s ON we.session_id = s.id GROUP BY s.browser ORDER BY avg_duration_sec DESC, browser LIMIT 10;
