SELECT event_type, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(event_type))) event_type_values, ROUND(STDDEV(duration_sec), 2) stddev_duration_sec FROM web_events GROUP BY event_type
