SELECT c.id, c.email, c.age FROM customers c WHERE c.age IS NULL AND EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.customer_id IN (4, 7, 8, 35));
