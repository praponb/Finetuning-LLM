SELECT id, session_id, event_time, duration_sec, SUM(duration_sec) OVER (PARTITION BY session_id ORDER BY event_time, id) AS running_duration_sec FROM web_events WHERE event_type = 'search'
