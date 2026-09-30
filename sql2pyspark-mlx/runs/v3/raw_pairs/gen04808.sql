SELECT id, session_id, duration_sec, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec, id) AS row_num FROM web_events;
