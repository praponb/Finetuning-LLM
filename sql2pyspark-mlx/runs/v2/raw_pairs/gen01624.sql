WITH filtered_web_events AS (
    SELECT *, COALESCE(page, 'Unknown') AS page_filled
    FROM web_events
    WHERE duration_sec BETWEEN 268 AND 465
),
summary AS (
    SELECT event_type, MAX(duration_sec) - MIN(duration_sec) AS duration_sec_range
    FROM filtered_web_events
    GROUP BY event_type
)
SELECT event_type, duration_sec_range
FROM summary
ORDER BY duration_sec_range DESC
