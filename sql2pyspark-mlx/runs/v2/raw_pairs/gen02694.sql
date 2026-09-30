SELECT * FROM (SELECT event_type, page, duration_sec FROM web_events) PIVOT (SUM(duration_sec) FOR page IN ('/product', '/home', '/search', '/checkout')) ORDER BY event_type;
