SELECT
    channel,
    status,
    customer_id,
    MAX(shipping_fee) AS max_shipping_fee,
    MAX(order_date) AS last_order_date,
    ROUND(AVG(discount), 2) AS avg_discount
FROM orders
WHERE MONTH(order_date) IN (4, 5, 8) AND shipping_fee < 39 AND shipping_fee <= 39
GROUP BY channel, status, customer_id
HAVING max_shipping_fee > 30
