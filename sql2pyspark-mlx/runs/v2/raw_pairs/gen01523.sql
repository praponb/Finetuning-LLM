SELECT s.id, s.started_at, s.country, s.device FROM sessions s WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE is_active);
