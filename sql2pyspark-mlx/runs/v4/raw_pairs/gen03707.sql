SELECT s.id, s.started_at
FROM sessions s
WHERE s.device = 'tablet' AND s.customer_id IN (SELECT id FROM customers WHERE NOT is_active)
