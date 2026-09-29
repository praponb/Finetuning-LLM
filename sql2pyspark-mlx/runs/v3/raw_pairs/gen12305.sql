SELECT id, session_id, event_time, duration_sec, SUM(duration_sec) OVER (PARTITION BY session_id ORDER BY event_time, id) running_duration_sec FROM web_events ORDER BY duration_sec DESC;
