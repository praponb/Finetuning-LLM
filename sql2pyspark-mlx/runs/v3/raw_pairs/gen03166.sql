WITH filtered_web_events AS (
    SELECT *, TRIM(page) AS clean_page
    FROM web_events
    WHERE duration_sec > 201 OR event_time >= '2023-08-10'
),
agg AS (
    SELECT event_type, COUNT(*) AS web_event_count
    FROM filtered_web_events
    GROUP BY event_type
)
SELECT event_type, web_event_count
FROM agg
ORDER BY event_type ASC
LIMIT 5;
