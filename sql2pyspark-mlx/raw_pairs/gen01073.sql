WITH ranked_stores AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft, id) AS pos
    FROM stores
    WHERE city IN ('New York', 'London')
)
SELECT *
FROM ranked_stores
WHERE pos = 1
ORDER BY sqft DESC
