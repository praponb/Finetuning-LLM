SELECT id, session_id, event_type FROM web_events WHERE duration_sec = 352 UNION SELECT id, session_id, event_type FROM web_events WHERE page IS NULL;
