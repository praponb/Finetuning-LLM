SELECT id, event_type, event_time
FROM (
    SELECT
        id,
        event_type,
        event_time,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time DESC, id) AS rn
    FROM web_events
) AS ranked
WHERE rn <= 1
ORDER BY id
