WITH latest AS (
    SELECT
        id,
        event_type,
        page,
        event_time,
        duration_sec
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type, session_id ORDER BY duration_sec DESC, id) AS rn
        FROM web_events
    ) ranked
    WHERE rn = 1
)
SELECT
    id,
    page,
    event_time,
    duration_sec
FROM latest
ORDER BY event_time DESC, id DESC
