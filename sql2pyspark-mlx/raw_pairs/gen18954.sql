SELECT orders.customer_id, orders.discount, customers.segment
FROM orders
LEFT JOIN customers ON orders.customer_id = customers.id
WHERE LOWER(TRIM(customers.email)) = 'bob.smith@yahoo.com'
ORDER BY orders.discount, customer_id;
