SELECT * FROM (SELECT page, event_type, duration_sec FROM web_events) PIVOT (SUM(duration_sec) FOR event_type IN ('add_to_cart', 'purchase', 'search', 'click')) ORDER BY page
