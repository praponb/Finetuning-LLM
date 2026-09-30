WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time DESC, id) AS row_num
    FROM web_events
)
SELECT *
FROM numbered
WHERE row_num <= 3;
