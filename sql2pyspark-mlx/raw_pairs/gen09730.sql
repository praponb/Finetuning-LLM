SELECT
    customers.city,
    customers.country,
    orders.amount,
    orders.shipping_fee,
    SUBSTRING(orders.channel, 1, 4) AS channel_prefix
FROM customers
LEFT JOIN orders ON customers.id = orders.customer_id
WHERE orders.status IN ('SHIPPED', 'PENDING', 'RETURNED');
