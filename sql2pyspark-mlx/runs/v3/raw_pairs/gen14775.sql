SELECT c.id, c.age, c.is_active FROM customers c WHERE c.age >= 61 AND NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.discount < 0.16) ORDER BY c.age DESC, id LIMIT 5
