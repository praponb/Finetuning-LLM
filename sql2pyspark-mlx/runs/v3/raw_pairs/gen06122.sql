WITH ranked AS (
    SELECT *, RANK() OVER (PARTITION BY session_id ORDER BY event_time) pos
    FROM web_events
    WHERE duration_sec >= 208
)
SELECT *
FROM ranked
WHERE pos <= 3
