SELECT s.id, s.country, s.device, s.started_at FROM sessions s WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE age = 46) ORDER BY country, s.id DESC
