SELECT
    payments.id,
    payments.method,
    payments.amount,
    orders.status,
    orders.channel,
    customers.age
FROM payments
INNER JOIN orders ON payments.order_id = orders.id
LEFT OUTER JOIN customers ON orders.customer_id = customers.id
