SELECT s.id, s.browser, s.device, s.customer_id FROM sessions s WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE LENGTH(name) >= 9);
