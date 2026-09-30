WITH store_summary AS (
    SELECT store_id, MAX(order_date) AS latest_order_date
    FROM orders
    GROUP BY store_id
)
SELECT s.city, s.region, ss.latest_order_date
FROM stores s
INNER JOIN store_summary ss ON s.id = ss.store_id
ORDER BY city
