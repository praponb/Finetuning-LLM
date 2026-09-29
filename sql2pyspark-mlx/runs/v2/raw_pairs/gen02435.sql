SELECT * FROM (SELECT event_type, page, duration_sec FROM web_events) PIVOT (MAX(duration_sec) FOR page IN ('/home', '/product', '/cart', '/search')) ORDER BY event_type;
