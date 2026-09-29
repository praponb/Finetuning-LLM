WITH RankedWebEvents AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY page ORDER BY duration_sec DESC) AS row_num
    FROM web_events
)
SELECT id, page, duration_sec
FROM RankedWebEvents
WHERE row_num <= 5
