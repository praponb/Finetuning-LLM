WITH filtered_suppliers AS (
    SELECT *
    FROM suppliers
    WHERE city IN ('Berlin', 'Tokyo', 'London')
),
summary AS (
    SELECT CASE WHEN rating >= 3.8 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END AS rating_bucket, SUM(CASE WHEN city = 'Chiang Mai' THEN 1 ELSE 0 END) AS chiang_mai_count, COUNT(*) AS num_suppliers
    FROM filtered_suppliers
    GROUP BY CASE WHEN rating >= 3.8 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM summary
WHERE num_suppliers < 6
ORDER BY chiang_mai_count DESC, num_suppliers, rating_bucket
LIMIT 10;
