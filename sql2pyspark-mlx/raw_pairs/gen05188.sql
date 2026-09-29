SELECT SUM(CASE WHEN page = '/product' THEN 1 ELSE 0 END) AS product_count, ROUND(AVG(duration_sec), 2) AS avg_duration_sec, SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list
FROM web_events
ORDER BY product_count DESC
