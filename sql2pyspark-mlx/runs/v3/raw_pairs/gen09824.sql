SELECT * FROM (SELECT event_type, page, duration_sec FROM web_events)
PIVOT (COUNT(duration_sec) FOR page IN ('/home', '/checkout', '/product', '/search'));
