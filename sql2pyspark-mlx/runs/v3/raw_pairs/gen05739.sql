SELECT * FROM (SELECT event_type, page, duration_sec FROM web_events WHERE event_time < '2025-01-01')
PIVOT (MAX(duration_sec) FOR page IN ('/cart', '/search', '/home'))
ORDER BY event_type
