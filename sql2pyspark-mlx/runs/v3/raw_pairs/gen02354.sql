SELECT s.id, s.customer_id, s.country
FROM sessions s
WHERE s.customer_id IN (SELECT id FROM customers WHERE NOT is_active)
