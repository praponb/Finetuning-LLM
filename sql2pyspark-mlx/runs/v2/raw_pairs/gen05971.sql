SELECT event_type, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(page))) AS page_values, COUNT(DISTINCT duration_sec) AS unique_event_types
FROM web_events
WHERE page IN ('/search', '/product', '/cart') OR TO_DATE(event_time) >= '2023-10-14'
GROUP BY event_type
HAVING COUNT(DISTINCT duration_sec) < 5;
