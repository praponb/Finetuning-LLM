WITH store_summary AS (
    SELECT store_id, COUNT(DISTINCT shipping_fee) AS unique_channels, COUNT(*) AS order_count
    FROM orders
    GROUP BY store_id
)
SELECT
    s.id,
    s.store_name,
    ss.unique_channels,
    ss.order_count
FROM stores s
LEFT JOIN store_summary ss ON s.id = ss.store_id
ORDER BY ss.unique_channels DESC
