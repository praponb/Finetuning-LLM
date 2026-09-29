SELECT id, session_id FROM web_events WHERE duration_sec BETWEEN 360 AND 443 UNION SELECT id, session_id FROM web_events WHERE session_id IN (3, 11, 25)
