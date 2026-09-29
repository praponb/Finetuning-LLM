WITH TopWebEvents AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time, id) AS pos
    FROM web_events
)
SELECT
    id,
    event_type,
    event_time,
    page
FROM TopWebEvents
WHERE pos <= 5;
