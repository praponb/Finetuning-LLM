WITH store_summary AS (
    SELECT store_id, MAX(order_date) AS latest_order_date
    FROM orders
    GROUP BY store_id
)
SELECT s.opened_date, s.store_name, ss.latest_order_date
FROM stores s
JOIN store_summary ss ON s.id = ss.store_id
ORDER BY ss.latest_order_date DESC;
