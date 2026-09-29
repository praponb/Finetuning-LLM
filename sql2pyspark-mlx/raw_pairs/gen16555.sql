SELECT c.id, c.name FROM customers c WHERE c.name LIKE 'FRA%' AND EXISTS (SELECT 1 FROM sessions s WHERE s.customer_id = c.id AND s.browser IS NOT NULL) ORDER BY id DESC LIMIT 5
