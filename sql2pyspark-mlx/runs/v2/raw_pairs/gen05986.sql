SELECT customers.id, customers.signup_date, customers.age FROM customers WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.customer_id = customers.id AND orders.store_id IS NOT NULL);
