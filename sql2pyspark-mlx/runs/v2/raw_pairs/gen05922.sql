SELECT s.device, COUNT(we.id) AS web_event_count FROM sessions s LEFT JOIN web_events we ON s.id = we.session_id WHERE s.customer_id IS NULL GROUP BY s.device ORDER BY s.device DESC
