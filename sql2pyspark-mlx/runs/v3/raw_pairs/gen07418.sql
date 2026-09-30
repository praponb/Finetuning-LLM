SELECT event_type, SORT_ARRAY(COLLECT_SET(page)) AS page_list
FROM web_events
WHERE event_type != 'add_to_cart' AND event_time < '2025-05-01'
GROUP BY event_type
ORDER BY event_type ASC;
