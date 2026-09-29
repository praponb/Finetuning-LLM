SELECT
    status,
    channel,
    CASE WHEN amount >= 1800 THEN 'high' WHEN amount >= 690 THEN 'mid' ELSE 'low' END AS amount_bucket,
    MAX(amount) - MIN(amount) AS amount_range,
    ROUND(AVG(shipping_fee), 2) AS avg_shipping_fee,
    MAX(order_date) AS last_order_date
FROM orders
WHERE MONTH(order_date) IN (1, 4, 5)
GROUP BY status, channel, CASE WHEN amount >= 1800 THEN 'high' WHEN amount >= 690 THEN 'mid' ELSE 'low' END
ORDER BY amount_bucket, status
