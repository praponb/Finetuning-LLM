WITH store_stats AS (
    SELECT store_id, AVG(amount) AS avg_amount
    FROM orders
    GROUP BY store_id
)
SELECT s.id, s.sqft, ss.avg_amount
FROM stores s
INNER JOIN store_stats ss ON s.id = ss.store_id
ORDER BY ss.avg_amount DESC, id
LIMIT 10
