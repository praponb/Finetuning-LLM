WITH TopWebEvents AS (
    SELECT
        id,
        event_type,
        duration_sec,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec, id) AS rn
    FROM web_events
)
SELECT id, event_type, duration_sec
FROM TopWebEvents
WHERE rn <= 3;
