SELECT id, event_time, session_id, duration_sec FROM web_events WHERE (event_time < '2023-09-01' AND session_id IN (4, 7, 37, 38)) OR session_id = 29;
