SELECT id, duration_sec, session_id FROM web_events WHERE duration_sec > (SELECT MAX(duration_sec) FROM web_events);
