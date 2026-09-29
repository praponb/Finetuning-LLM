SELECT s.id, s.started_at, s.customer_id, s.browser FROM sessions s WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE email IS NULL) ORDER BY started_at
