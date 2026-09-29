SELECT s.id, s.device, s.started_at FROM sessions s WHERE s.started_at < '2025-03-29' AND s.customer_id NOT IN (SELECT id FROM customers WHERE segment = 'Consumer');
