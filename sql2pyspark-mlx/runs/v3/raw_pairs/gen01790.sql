WITH top_web_events AS (
    SELECT
        id,
        page,
        event_time,
        event_type,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time, id) AS rnk
    FROM web_events
)
SELECT
    id,
    page,
    event_time,
    event_type
FROM top_web_events
WHERE rnk <= 3
