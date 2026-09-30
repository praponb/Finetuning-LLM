WITH ranked_web_events AS (
    SELECT
        id,
        event_type,
        event_time,
        duration_sec,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time DESC, id) AS rn
    FROM web_events
)
SELECT
    id,
    event_type,
    event_time,
    duration_sec
FROM ranked_web_events
WHERE rn <= 3;
