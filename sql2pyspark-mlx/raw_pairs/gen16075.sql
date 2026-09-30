SELECT
    channel,
    COUNT(*) AS order_count,
    COUNT(amount) AS amount_count,
    SUM(CASE WHEN status = 'COMPLETED' THEN shipping_fee ELSE 0 END) AS completed_shipping_fee
FROM orders
GROUP BY channel
ORDER BY amount_count, order_count DESC, channel
LIMIT 10
