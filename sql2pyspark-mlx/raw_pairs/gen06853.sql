SELECT * FROM (SELECT page, event_type, duration_sec FROM web_events WHERE page IN ('/cart', '/checkout')) PIVOT (COUNT(duration_sec) FOR event_type IN ('page_view', 'search')) ORDER BY page;
