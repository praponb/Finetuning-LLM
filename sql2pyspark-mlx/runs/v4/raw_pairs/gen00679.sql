SELECT
    orders.id,
    orders.store_id,
    orders.status,
    orders.order_date,
    customers.country,
    customers.name,
    customers.signup_date
FROM orders
JOIN customers ON orders.customer_id = customers.id
ORDER BY store_id DESC, id
LIMIT 20
