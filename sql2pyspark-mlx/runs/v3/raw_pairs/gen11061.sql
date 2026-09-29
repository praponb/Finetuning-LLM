WITH TopWebEvents AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec, id) AS row_num
    FROM web_events
    WHERE page = '/checkout'
)
SELECT
    id,
    event_type,
    duration_sec,
    session_id
FROM TopWebEvents
WHERE row_num <= 1;
