SELECT s.id, COUNT(we.id) AS web_event_count, SUM(we.duration_sec) AS total_duration_sec FROM sessions s INNER JOIN web_events we ON s.id = we.session_id GROUP BY s.id
