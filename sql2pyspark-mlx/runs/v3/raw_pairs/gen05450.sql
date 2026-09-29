SELECT
    shipments.shipped_date,
    shipments.status,
    orders.customer_id,
    orders.discount,
    customers.email,
    customers.is_active
FROM shipments
JOIN orders ON shipments.order_id = orders.id
JOIN customers ON orders.customer_id = customers.id
WHERE YEAR(customers.signup_date) = 2023;
