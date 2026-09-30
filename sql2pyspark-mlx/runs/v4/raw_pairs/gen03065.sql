SELECT c.id, c.age, c.segment FROM customers c WHERE c.segment = 'Consumer' AND EXISTS (SELECT 1 FROM sessions s WHERE s.customer_id = c.id AND s.device = 'mobile')
