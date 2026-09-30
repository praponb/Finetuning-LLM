WITH store_stats AS (
    SELECT store_id, SUM(CASE WHEN channel = 'web' THEN amount ELSE 0 END) web_amount, MIN(shipping_fee) min_shipping_fee
    FROM orders
    GROUP BY store_id
)
SELECT
    s.id,
    s.city,
    ss.web_amount,
    ss.min_shipping_fee
FROM stores s
JOIN store_stats ss ON s.id = ss.store_id
ORDER BY web_amount, id
LIMIT 10;
