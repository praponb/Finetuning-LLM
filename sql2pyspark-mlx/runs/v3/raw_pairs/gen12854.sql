WITH numbered AS (
    SELECT
        id,
        event_type,
        duration_sec,
        event_time,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec DESC, id) AS rn
    FROM web_events
)
SELECT
    id,
    event_type,
    duration_sec,
    event_time
FROM numbered
WHERE rn <= 2
ORDER BY id DESC;
