SELECT
    status,
    MONTH(order_date) AS order_month,
    CASE WHEN shipping_fee >= 36 THEN 'high' WHEN shipping_fee >= 20 THEN 'mid' ELSE 'low' END AS shipping_fee_bucket,
    COUNT(CASE WHEN status = 'PENDING' THEN 1 END) AS pending_count,
    MAX(order_date) AS last_order_date
FROM orders
WHERE NOT (status = 'COMPLETED' OR store_id = 10)
GROUP BY status, order_month, shipping_fee_bucket
ORDER BY 4
