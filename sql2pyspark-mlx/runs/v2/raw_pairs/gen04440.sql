SELECT s.id, s.country, s.started_at FROM sessions s WHERE s.customer_id IN (SELECT id FROM customers WHERE is_active) ORDER BY 3 DESC, id LIMIT 10
