SELECT id, duration_sec, page, session_id FROM web_events WHERE duration_sec >= (SELECT MAX(duration_sec) FROM web_events WHERE event_time >= '2023-11-01')
