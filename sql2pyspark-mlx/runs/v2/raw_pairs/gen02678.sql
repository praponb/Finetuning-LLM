WITH Ranked AS (
    SELECT
        id,
        country,
        started_at,
        DENSE_RANK() OVER (PARTITION BY country ORDER BY started_at DESC) AS rnk
    FROM sessions
    WHERE browser = 'Safari'
)
SELECT *
FROM Ranked
WHERE rnk <= 3;
