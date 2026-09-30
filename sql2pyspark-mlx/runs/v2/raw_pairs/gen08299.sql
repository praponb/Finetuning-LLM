SELECT c.id, c.segment, c.credit_limit FROM customers c WHERE c.email LIKE '%@gmail.com' AND NOT EXISTS (SELECT 1 FROM sessions s WHERE s.customer_id = c.id AND s.browser = 'Safari');
