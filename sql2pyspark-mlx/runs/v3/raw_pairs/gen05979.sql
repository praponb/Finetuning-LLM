SELECT s.id, s.customer_id FROM sessions s WHERE s.customer_id IN (SELECT id FROM customers WHERE is_active);
