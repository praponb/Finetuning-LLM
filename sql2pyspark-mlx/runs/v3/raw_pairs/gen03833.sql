SELECT
    customers.email,
    customers.segment,
    customers.signup_date,
    orders.status
FROM customers
LEFT JOIN orders ON orders.customer_id = customers.id
WHERE orders.order_date >= '2023-07-18'
