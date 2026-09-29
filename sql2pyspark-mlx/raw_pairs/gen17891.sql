SELECT s.browser, COUNT(we.id) AS web_event_count, ROUND(AVG(we.duration_sec), 2) AS avg_duration_sec FROM sessions s LEFT OUTER JOIN web_events we ON s.id = we.session_id GROUP BY s.browser
