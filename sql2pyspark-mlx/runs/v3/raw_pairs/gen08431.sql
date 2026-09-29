SELECT
    orders.amount,
    customers.age,
    customers.segment,
    customers.email
FROM orders
INNER JOIN customers ON orders.customer_id = customers.id
