SELECT s.id, s.started_at, s.country FROM sessions s WHERE s.id IN (SELECT session_id FROM web_events WHERE session_id IN (4, 18, 23, 34)) ORDER BY s.country DESC, id LIMIT 1
