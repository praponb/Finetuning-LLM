SELECT session_id, duration_sec FROM web_events WHERE event_time < '2023-03-21' AND duration_sec > 380 ORDER BY duration_sec DESC, session_id;
