SELECT
    payments.amount,
    orders.discount,
    orders.shipping_fee,
    customers.country,
    customers.email
FROM payments
LEFT JOIN orders ON orders.id = payments.order_id
RIGHT JOIN customers ON orders.customer_id = customers.id
ORDER BY orders.discount DESC;
