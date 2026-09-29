WITH ranked AS (
    SELECT
        id,
        event_type,
        duration_sec,
        session_id,
        RANK() OVER (PARTITION BY event_type ORDER BY duration_sec DESC) AS pos
    FROM web_events
)
SELECT
    id,
    event_type,
    duration_sec,
    session_id
FROM ranked
WHERE pos <= 5
ORDER BY duration_sec DESC;
