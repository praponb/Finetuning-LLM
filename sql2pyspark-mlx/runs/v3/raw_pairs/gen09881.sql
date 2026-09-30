SELECT MAX(duration_sec) AS max_duration_sec, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(event_type))) AS event_type_values, COUNT(*) AS row_count
FROM web_events
WHERE page = '/search' AND event_type NOT IN ('add_to_cart', 'search')
ORDER BY max_duration_sec DESC;
