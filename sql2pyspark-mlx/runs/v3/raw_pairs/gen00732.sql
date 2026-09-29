SELECT * FROM web_events WHERE TO_DATE(event_time) < '2023-07-01' AND (page IN ('/product', '/checkout') OR TO_DATE(event_time) >= '2025-03-29');
