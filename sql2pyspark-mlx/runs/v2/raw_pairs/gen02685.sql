WITH ranked AS (
    SELECT
        id,
        page,
        duration_sec,
        event_time,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec, id) AS rnk
    FROM web_events
)
SELECT
    id,
    page,
    duration_sec,
    event_time
FROM ranked
WHERE rnk = 1
ORDER BY duration_sec DESC;
