SELECT id, session_id FROM web_events WHERE session_id IN (37, 38) UNION ALL SELECT id, session_id FROM web_events WHERE session_id IN (1, 7, 10, 23);
