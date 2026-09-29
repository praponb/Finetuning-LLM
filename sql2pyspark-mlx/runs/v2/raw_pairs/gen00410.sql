SELECT MIN(duration_sec) AS min_duration_sec, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(event_type))) AS event_type_values, COUNT(DISTINCT duration_sec) AS unique_event_types
FROM web_events
WHERE event_type = 'page_view' AND duration_sec BETWEEN 144 AND 445 AND duration_sec BETWEEN 280 AND 425
ORDER BY event_type_values
