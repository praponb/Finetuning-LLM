SELECT
    orders.id,
    orders.discount,
    orders.status,
    customers.credit_limit,
    customers.country,
    customers.age
FROM orders
JOIN customers ON orders.customer_id = customers.id
WHERE orders.customer_id = 1 OR customers.age BETWEEN 56 AND 57
ORDER BY 3;
