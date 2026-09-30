WITH Ranked AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY city ORDER BY opened_date DESC) AS rnk
    FROM stores
)
SELECT
    id,
    city,
    opened_date,
    sqft
FROM Ranked
WHERE rnk <= 3
