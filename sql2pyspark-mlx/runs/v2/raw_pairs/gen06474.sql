SELECT duration_sec, session_id FROM web_events WHERE duration_sec = 301 UNION ALL SELECT duration_sec, session_id FROM web_events WHERE session_id IN (1, 11, 20, 33)
