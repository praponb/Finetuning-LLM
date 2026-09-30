WITH RankedWebEvents AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec DESC, id) AS rnk
    FROM web_events
)
SELECT id, page, duration_sec
FROM RankedWebEvents
WHERE rnk <= 3
ORDER BY id DESC;
