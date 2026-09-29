SELECT SUM(CASE WHEN page = '/product' THEN 1 ELSE 0 END) AS product_count
FROM web_events
WHERE event_type = 'search'
ORDER BY product_count DESC;
