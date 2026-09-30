SELECT id, session_id, duration_sec FROM web_events WHERE session_id IN (8, 33) UNION ALL SELECT id, session_id, duration_sec FROM web_events WHERE duration_sec IS NOT NULL
