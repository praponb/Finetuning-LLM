WITH ranked_web_events AS (
    SELECT
        id,
        page,
        duration_sec,
        event_time,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec DESC, id) AS row_num
    FROM web_events
)
SELECT
    id,
    page,
    duration_sec,
    event_time
FROM ranked_web_events
WHERE row_num <= 2
ORDER BY id DESC
