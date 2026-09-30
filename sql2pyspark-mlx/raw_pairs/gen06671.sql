SELECT * FROM web_events WHERE event_time < '2023-12-14' AND page IN ('/product', '/cart') AND session_id IN (6, 13, 15, 35) ORDER BY event_time
