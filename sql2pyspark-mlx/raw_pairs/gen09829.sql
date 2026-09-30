SELECT CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(event_type))) AS event_type_values, SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list
FROM web_events
WHERE session_id IN (10, 21) AND event_type IN ('search', 'add_to_cart') AND TO_DATE(event_time) >= '2025-01-08'
ORDER BY event_type_values DESC
