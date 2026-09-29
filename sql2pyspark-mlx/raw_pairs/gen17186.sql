SELECT customers.id, customers.is_active
FROM customers
WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.customer_id = customers.id AND orders.amount BETWEEN 430 AND 1050)
