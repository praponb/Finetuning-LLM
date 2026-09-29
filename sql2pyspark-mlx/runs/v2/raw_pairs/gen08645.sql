SELECT id, duration_sec, event_time, event_type FROM web_events WHERE duration_sec >= (SELECT MAX(duration_sec) FROM web_events WHERE session_id IN (23, 28)) ORDER BY id DESC LIMIT 10
