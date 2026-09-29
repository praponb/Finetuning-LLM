SELECT s.id, s.customer_id, s.browser FROM sessions s WHERE s.browser IS NOT NULL AND s.customer_id NOT IN (SELECT id FROM customers WHERE age = 65)
