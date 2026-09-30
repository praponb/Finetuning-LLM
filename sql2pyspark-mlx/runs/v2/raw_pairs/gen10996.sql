WITH store_summary AS (
    SELECT store_id, COUNT(*) AS cnt, SUM(shipping_fee) AS total_shipping_fee
    FROM orders
    WHERE customer_id IN (5, 9, 14)
    GROUP BY store_id
)
SELECT
    s.id,
    s.region,
    s.city,
    ss.cnt,
    ss.total_shipping_fee
FROM stores s
INNER JOIN store_summary ss ON s.id = ss.store_id
ORDER BY region DESC
