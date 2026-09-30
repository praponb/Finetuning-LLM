WITH top_web_events AS (
    SELECT
        id,
        event_type,
        duration_sec,
        session_id,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec DESC, id) AS pos
    FROM web_events
)
SELECT
    id,
    event_type,
    duration_sec,
    session_id
FROM top_web_events
WHERE pos <= 3
