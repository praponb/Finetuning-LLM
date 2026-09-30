WITH Ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC, id) AS rn
    FROM sessions
)
SELECT *
FROM Ranked
WHERE rn = 1;
