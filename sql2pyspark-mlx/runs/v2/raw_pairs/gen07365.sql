SELECT s.country, MIN(we.duration_sec) min_duration_sec FROM web_events we LEFT JOIN sessions s ON s.id = we.session_id GROUP BY s.country ORDER BY country
