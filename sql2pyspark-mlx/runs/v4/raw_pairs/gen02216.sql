SELECT s.id, s.device, s.customer_id, s.browser FROM sessions s WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE email IS NOT NULL);
