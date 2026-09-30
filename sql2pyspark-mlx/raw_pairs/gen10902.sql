SELECT event_day, max_duration_sec, page_values
FROM (
    SELECT TO_DATE(event_time) AS event_day, MAX(duration_sec) AS max_duration_sec, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(page))) AS page_values
    FROM web_events
    GROUP BY TO_DATE(event_time)
) AS s
WHERE max_duration_sec > 390
ORDER BY max_duration_sec, page_values, event_day
LIMIT 5;
