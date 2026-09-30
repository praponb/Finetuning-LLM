SELECT c.id, c.credit_limit, c.is_active FROM customers c WHERE NOT EXISTS (SELECT 1 FROM sessions s WHERE s.customer_id = c.id);
