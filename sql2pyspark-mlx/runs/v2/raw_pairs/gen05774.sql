SELECT id, event_type, COUNT(*) OVER (PARTITION BY event_type) AS event_type_count FROM web_events WHERE session_id IN (13, 39) ORDER BY id DESC
