SELECT id, session_id, page, duration_sec, MAX(duration_sec) OVER (PARTITION BY session_id, page) AS session_id_max_duration_sec FROM web_events
