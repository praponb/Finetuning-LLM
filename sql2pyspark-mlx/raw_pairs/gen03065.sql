WITH filtered_web_events AS (
    SELECT *
    FROM web_events
    WHERE duration_sec <= 243 AND (duration_sec BETWEEN 294 AND 447 OR HOUR(event_time) BETWEEN 11 AND 15)
),
web_events_stats AS (
    SELECT event_type, ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec, COUNT(CASE WHEN event_type = 'page_view' THEN 1 END) AS page_view_count
    FROM filtered_web_events
    GROUP BY event_type
)
SELECT event_type, page_view_count
FROM web_events_stats
ORDER BY page_view_count DESC;
