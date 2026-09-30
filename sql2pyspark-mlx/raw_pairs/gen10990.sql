SELECT id, session_id, duration_sec, RANK() OVER (PARTITION BY session_id ORDER BY duration_sec DESC) AS duration_sec_rank FROM web_events WHERE duration_sec != 303
