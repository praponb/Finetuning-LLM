SELECT c.id, c.is_active FROM customers c WHERE c.country = 'TH' AND NOT EXISTS (SELECT 1 FROM sessions s WHERE s.customer_id = c.id)
