SELECT customers.id, customers.is_active FROM customers WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.customer_id = customers.id AND orders.store_id IN (7, 8, 21, 29)) ORDER BY id DESC;
