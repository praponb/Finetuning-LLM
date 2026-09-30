WITH Ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft, id) AS rn
    FROM stores
)
SELECT id, region, sqft
FROM Ranked
WHERE rn = 1
ORDER BY id DESC
