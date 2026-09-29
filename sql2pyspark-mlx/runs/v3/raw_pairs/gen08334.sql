SELECT o.id, o.discount, o.customer_id, o.amount FROM orders o WHERE o.store_id IS NULL AND o.customer_id IN (SELECT id FROM customers WHERE name LIKE '%Chen%');
