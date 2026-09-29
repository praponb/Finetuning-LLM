SELECT c.id, c.credit_limit, c.segment, c.city FROM customers c WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id);
