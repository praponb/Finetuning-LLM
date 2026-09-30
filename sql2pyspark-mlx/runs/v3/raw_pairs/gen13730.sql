SELECT page, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(event_type))) AS event_type_values, ROUND(AVG(duration_sec), 2) AS avg_duration_sec
FROM web_events
GROUP BY page
