SELECT id, event_type, session_id FROM web_events WHERE duration_sec BETWEEN 132 AND 420 EXCEPT SELECT id, event_type, session_id FROM web_events WHERE event_type = 'purchase' ORDER BY session_id
