WITH numbered AS (
    SELECT *, RANK() OVER (PARTITION BY event_type ORDER BY duration_sec DESC) AS pos
    FROM web_events
)
SELECT *
FROM numbered
WHERE pos <= 3
