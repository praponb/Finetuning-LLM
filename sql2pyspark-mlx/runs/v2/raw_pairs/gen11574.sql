WITH top_web_events AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time, id) pos
    FROM web_events
)
SELECT *
FROM top_web_events
WHERE pos <= 5;
