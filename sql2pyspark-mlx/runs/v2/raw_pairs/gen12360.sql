SELECT * FROM (SELECT event_type, page, duration_sec FROM web_events)
PIVOT (COUNT(duration_sec) FOR page IN ('/product', '/cart', '/checkout', '/search'))
ORDER BY event_type
