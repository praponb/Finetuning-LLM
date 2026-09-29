SELECT s.id, s.started_at
FROM sessions s
WHERE s.customer_id IN (SELECT id FROM customers WHERE age <> 48)
ORDER BY id;
