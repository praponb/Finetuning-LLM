SELECT id, session_id FROM web_events WHERE duration_sec BETWEEN 215 AND 468 INTERSECT SELECT id, session_id FROM web_events WHERE event_type = 'add_to_cart'
