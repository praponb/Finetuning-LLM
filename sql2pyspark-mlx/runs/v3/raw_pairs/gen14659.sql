WITH numbered AS (
    SELECT
        id,
        event_type,
        event_time,
        DENSE_RANK() OVER (PARTITION BY event_type ORDER BY event_time DESC) AS pos
    FROM web_events
)
SELECT id, event_type, event_time
FROM numbered
WHERE pos <= 1
ORDER BY event_time DESC, id;
