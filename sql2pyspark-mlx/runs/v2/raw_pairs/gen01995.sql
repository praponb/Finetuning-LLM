WITH filtered_web_events AS (
    SELECT *, HOUR(event_time) AS event_time_hour
    FROM web_events
    WHERE session_id IN (13, 24, 39)
),
page_summary AS (
    SELECT page, HOUR(event_time) AS event_hour, MAX(event_time) AS last_event_time
    FROM filtered_web_events
    GROUP BY page, HOUR(event_time)
)
SELECT page, event_hour, last_event_time
FROM page_summary
ORDER BY 3, event_hour DESC, page
LIMIT 50;
