WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec DESC, id) AS row_num
    FROM web_events
)
SELECT
    id,
    event_type,
    duration_sec,
    page
FROM numbered
WHERE row_num = 1;
