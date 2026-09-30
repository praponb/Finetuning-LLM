SELECT
    orders.amount,
    orders.discount,
    orders.store_id,
    customers.is_active,
    customers.name,
    customers.city
FROM orders
RIGHT JOIN customers ON orders.customer_id = customers.id
ORDER BY orders.amount DESC
