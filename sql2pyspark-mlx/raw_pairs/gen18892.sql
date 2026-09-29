WITH recent_web_events AS (
    SELECT *
    FROM web_events
    WHERE duration_sec <= 452 AND event_time >= '2024-05-20' AND duration_sec BETWEEN 162 AND 175
),
summary AS (
    SELECT event_type, MAX(event_time) AS last_event_time, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(page))) AS page_values
    FROM recent_web_events
    GROUP BY event_type
)
SELECT event_type, last_event_time, page_values
FROM summary
ORDER BY last_event_time DESC;
