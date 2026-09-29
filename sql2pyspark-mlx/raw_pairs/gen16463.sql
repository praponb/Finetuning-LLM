WITH ranked_web_events AS (
    SELECT
        id,
        event_type,
        event_time,
        duration_sec,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time, id) AS row_num
    FROM web_events
)
SELECT *
FROM ranked_web_events
WHERE row_num <= 3
ORDER BY id DESC
