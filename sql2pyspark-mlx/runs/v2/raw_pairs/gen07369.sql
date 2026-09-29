SELECT * FROM (SELECT event_type, page, duration_sec FROM web_events WHERE duration_sec IS NULL) PIVOT (AVG(duration_sec) FOR page IN ('/cart', '/checkout')) ORDER BY event_type
