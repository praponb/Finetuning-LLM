SELECT
    orders.channel,
    COUNT(payments.id) AS payment_count,
    COUNT(DISTINCT payments.amount) AS unique_statuss,
    ROUND(STDDEV(payments.amount), 2) AS stddev_amount
FROM orders
LEFT JOIN payments ON orders.id = payments.order_id
WHERE payments.amount <= 1260
GROUP BY orders.channel
ORDER BY 1
