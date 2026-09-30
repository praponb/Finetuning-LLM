WITH RankedWebEvents AS (
    SELECT
        id,
        page,
        duration_sec,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec DESC, id) AS row_num
    FROM web_events
)
SELECT id, page, duration_sec
FROM RankedWebEvents
WHERE row_num <= 2
ORDER BY id DESC
