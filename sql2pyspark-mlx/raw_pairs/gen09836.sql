SELECT * FROM (SELECT page, event_type, duration_sec FROM web_events WHERE page = '/home') PIVOT (COUNT(duration_sec) FOR event_type IN ('add_to_cart', 'search')) ORDER BY page;
