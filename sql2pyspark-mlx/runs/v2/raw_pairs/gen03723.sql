WITH store_stats AS (
    SELECT store_id, MAX(shipping_fee) - MIN(shipping_fee) AS shipping_fee_range, COUNT(*) AS cnt
    FROM orders
    GROUP BY store_id
)
SELECT s.opened_date, ss.shipping_fee_range, ss.cnt
FROM stores s
LEFT OUTER JOIN store_stats ss ON s.id = ss.store_id
