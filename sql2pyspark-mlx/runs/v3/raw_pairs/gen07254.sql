SELECT * FROM (SELECT event_type, page, duration_sec FROM web_events) PIVOT (MAX(duration_sec) FOR page IN ('/checkout', '/search')) ORDER BY event_type
