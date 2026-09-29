SELECT * FROM (SELECT event_type, page, duration_sec FROM web_events WHERE duration_sec >= 172) PIVOT (MAX(duration_sec) FOR page IN ('/checkout', '/cart', '/search', '/product'))
