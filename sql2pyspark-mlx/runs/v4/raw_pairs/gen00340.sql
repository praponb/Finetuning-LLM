SELECT id, event_type, duration_sec, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec DESC, id) rn FROM web_events WHERE session_id IN (5, 14);
