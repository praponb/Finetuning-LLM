SELECT s.id, s.device, s.country FROM sessions s WHERE s.customer_id IN (SELECT id FROM customers WHERE segment IN ('SMB', 'Consumer'));
