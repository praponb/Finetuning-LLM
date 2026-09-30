WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec DESC, id) AS rnk
    FROM web_events
)
SELECT id, event_type, duration_sec
FROM numbered
WHERE rnk <= 1
ORDER BY id DESC
