SELECT * FROM (SELECT event_type, page, duration_sec FROM web_events) PIVOT (COUNT(duration_sec) FOR page IN ('/cart', '/checkout')) ORDER BY event_type;
