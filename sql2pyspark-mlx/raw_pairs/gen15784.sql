WITH store_summary AS (
    SELECT store_id, COUNT(*) order_count
    FROM orders
    GROUP BY store_id
)
SELECT
    s.id,
    s.region,
    s.opened_date,
    ss.order_count
FROM stores s
LEFT JOIN store_summary ss ON s.id = ss.store_id
ORDER BY opened_date
