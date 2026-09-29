SELECT c.id, c.is_active
FROM customers c
WHERE c.age >= 57 AND c.id IN (SELECT customer_id FROM sessions)
ORDER BY id
LIMIT 1
