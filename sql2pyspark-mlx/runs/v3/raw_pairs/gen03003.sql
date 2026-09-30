WITH store_stats AS (
    SELECT store_id, MIN(order_date) min_order_date
    FROM orders
    WHERE status IN ('COMPLETED', 'RETURNED')
    GROUP BY store_id
)
SELECT s.region, ss.min_order_date
FROM stores s
INNER JOIN store_stats ss ON s.id = ss.store_id
ORDER BY region, min_order_date
