WITH Ranked AS (
    SELECT *, RANK() OVER (PARTITION BY country ORDER BY started_at DESC) AS pos
    FROM sessions
    WHERE started_at >= '2023-06-01'
)
SELECT *
FROM Ranked
WHERE pos = 1
