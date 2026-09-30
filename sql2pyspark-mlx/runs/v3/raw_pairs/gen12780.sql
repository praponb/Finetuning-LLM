SELECT c.id, c.is_active, c.name FROM customers c WHERE EXISTS (SELECT 1 FROM sessions s WHERE s.customer_id = c.id);
