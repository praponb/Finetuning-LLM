SELECT s.id, s.started_at FROM sessions s WHERE s.customer_id IN (SELECT id FROM customers WHERE is_active = FALSE) ORDER BY id LIMIT 5
