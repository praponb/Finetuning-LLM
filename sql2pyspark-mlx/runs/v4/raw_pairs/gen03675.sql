SELECT c.id, c.name, c.is_active, c.city FROM customers c WHERE c.segment <> 'Consumer' AND EXISTS (SELECT 1 FROM sessions s WHERE s.customer_id = c.id) ORDER BY city DESC, 1 DESC
