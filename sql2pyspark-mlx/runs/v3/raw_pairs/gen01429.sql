SELECT id, session_id, duration_sec, MIN(duration_sec) OVER (PARTITION BY session_id) AS session_id_min_duration_sec FROM web_events
