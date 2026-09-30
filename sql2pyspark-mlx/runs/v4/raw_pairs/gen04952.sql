SELECT
    orders.id,
    orders.channel,
    orders.status,
    customers.name
FROM orders
JOIN customers ON orders.customer_id = customers.id
WHERE customers.city IN ('London', 'Chiang Mai');
