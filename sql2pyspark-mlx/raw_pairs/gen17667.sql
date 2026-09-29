SELECT id, page, session_id, event_time, duration_sec, LAG(duration_sec, 1) OVER (PARTITION BY page, session_id ORDER BY event_time, id) AS prev_duration_sec FROM web_events
