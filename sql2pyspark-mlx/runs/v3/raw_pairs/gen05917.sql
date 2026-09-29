SELECT page, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(page))) AS page_values
FROM web_events
WHERE duration_sec < 469 AND page IN ('/search', '/product', '/cart') AND TO_DATE(event_time) >= '2023-10-14'
GROUP BY page;
