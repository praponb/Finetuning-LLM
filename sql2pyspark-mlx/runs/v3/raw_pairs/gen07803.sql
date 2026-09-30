SELECT TO_DATE(event_time) AS event_day, SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list
FROM web_events
GROUP BY TO_DATE(event_time)
ORDER BY event_day;
