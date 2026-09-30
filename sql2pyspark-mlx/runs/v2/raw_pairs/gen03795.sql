SELECT event_type, event_time, duration_sec
FROM web_events
WHERE session_id = 20 AND page IN ('/search', '/home', '/product')
