WITH Ranked AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY region ORDER BY sqft) AS rnk
    FROM stores
)
SELECT id, region, sqft
FROM Ranked
WHERE rnk <= 3
ORDER BY id DESC, region;
