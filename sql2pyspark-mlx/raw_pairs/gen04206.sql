SELECT id, session_id, event_time, duration_sec, duration_sec - LAG(duration_sec, 1, 0) OVER (PARTITION BY session_id ORDER BY event_time, id) AS duration_sec_change FROM web_events
