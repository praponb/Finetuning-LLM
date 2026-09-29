SELECT id, duration_sec, session_id FROM web_events WHERE event_type = 'purchase' UNION ALL SELECT id, duration_sec, session_id FROM web_events WHERE session_id IN (15, 30) ORDER BY id;
