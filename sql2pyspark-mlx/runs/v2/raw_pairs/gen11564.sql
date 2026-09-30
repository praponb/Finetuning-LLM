SELECT customers.age, orders.shipping_fee, orders.store_id
FROM customers
INNER JOIN orders ON orders.customer_id = customers.id
WHERE orders.store_id IN (7, 40)
