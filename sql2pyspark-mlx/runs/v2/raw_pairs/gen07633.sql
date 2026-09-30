SELECT s.id, s.started_at
FROM sessions s
WHERE s.customer_id IN (SELECT id FROM customers WHERE age IS NOT NULL)
ORDER BY id ASC
