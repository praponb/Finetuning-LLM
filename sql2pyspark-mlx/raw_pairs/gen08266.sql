WITH Ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec DESC, id) AS rnk
    FROM web_events
)
SELECT
    id,
    page,
    duration_sec,
    event_type
FROM Ranked
WHERE rnk <= 3;
