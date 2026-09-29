WITH store_stats AS (
    SELECT store_id, COUNT(*) AS num_orders
    FROM orders
    GROUP BY store_id
)
SELECT
    s.id,
    s.opened_date,
    s.store_name,
    ss.num_orders
FROM stores s
JOIN store_stats ss ON s.id = ss.store_id
WHERE ss.num_orders > 8
ORDER BY store_name DESC;
