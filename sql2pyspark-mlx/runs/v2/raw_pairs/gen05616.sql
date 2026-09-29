SELECT c.id, c.name, c.segment, c.credit_limit FROM customers c WHERE EXISTS (SELECT 1 FROM sessions s WHERE s.customer_id = c.id AND s.browser = 'Chrome')
