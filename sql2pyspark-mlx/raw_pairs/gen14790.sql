SELECT s.id, s.country, s.browser, s.started_at FROM sessions s WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE email IS NOT NULL) ORDER BY id DESC;
