SELECT id, event_type, duration_sec FROM web_events WHERE event_type = 'purchase' UNION ALL SELECT id, event_type, duration_sec FROM web_events WHERE session_id = 5;
