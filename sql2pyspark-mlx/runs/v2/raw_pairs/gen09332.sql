SELECT s.country, COUNT(we.id) AS web_event_count FROM sessions s LEFT JOIN web_events we ON s.id = we.session_id WHERE we.session_id IN (16, 19, 30, 37) GROUP BY s.country HAVING COUNT(we.id) > 3
