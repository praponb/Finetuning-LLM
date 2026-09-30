WITH store_stats AS (
    SELECT store_id, SUM(shipping_fee) total_shipping_fee
    FROM orders
    WHERE store_id IS NOT NULL
    GROUP BY store_id
)
SELECT
    s.id,
    s.store_name,
    s.region,
    ss.total_shipping_fee
FROM stores s
INNER JOIN store_stats ss ON s.id = ss.store_id
WHERE ss.total_shipping_fee >= 210
ORDER BY s.store_name ASC, id ASC
LIMIT 5;
