SELECT s.id, s.device, s.started_at FROM sessions s WHERE s.customer_id = 38 AND s.customer_id NOT IN (SELECT id FROM customers WHERE is_active)
