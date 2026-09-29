SELECT s.id, s.device, COUNT(we.id) AS web_event_count FROM sessions s LEFT OUTER JOIN web_events we ON we.session_id = s.id GROUP BY s.id, s.device ORDER BY s.device, id LIMIT 5;
