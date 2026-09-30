SELECT id, session_id, event_type, COUNT(*) OVER (PARTITION BY session_id, event_type) AS session_id_count FROM web_events WHERE page <> '/home'
