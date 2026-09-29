SELECT * FROM (SELECT page, event_type, duration_sec FROM web_events) PIVOT (AVG(duration_sec) FOR event_type IN ('purchase', 'add_to_cart')) ORDER BY page;
