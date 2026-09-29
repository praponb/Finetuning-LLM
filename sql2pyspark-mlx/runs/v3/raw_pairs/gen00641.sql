SELECT id, session_id, duration_sec, event_time, FIRST_VALUE(event_time) OVER (PARTITION BY session_id ORDER BY duration_sec, id) AS first_event_time FROM web_events
