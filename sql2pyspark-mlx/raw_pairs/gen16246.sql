WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time DESC, id) row_num
    FROM web_events
)
SELECT *
FROM ranked
WHERE row_num <= 2;
