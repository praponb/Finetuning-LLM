SELECT page, event_type, SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list
FROM web_events
WHERE duration_sec >= 222 AND page = '/checkout' AND page IN ('/product', '/search', '/checkout')
GROUP BY page, event_type
ORDER BY event_type, page DESC;
