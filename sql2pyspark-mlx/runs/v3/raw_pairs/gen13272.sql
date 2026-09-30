SELECT * FROM (SELECT event_type, page, duration_sec FROM web_events)
PIVOT (AVG(duration_sec) FOR page IN ('/home', '/checkout', '/product', '/cart'))
ORDER BY event_type
