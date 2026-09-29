WITH Ranked AS (
    SELECT
        id,
        page,
        duration_sec,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec, id) AS rnk
    FROM web_events
    WHERE event_time < '2023-09-01'
)
SELECT id, page, duration_sec
FROM Ranked
WHERE rnk <= 3;
