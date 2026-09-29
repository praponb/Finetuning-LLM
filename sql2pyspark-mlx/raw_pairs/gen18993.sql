WITH ranked_stores AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY region ORDER BY sqft DESC) AS pos
    FROM stores
)
SELECT id, region, sqft
FROM ranked_stores
WHERE pos = 1
ORDER BY sqft DESC
