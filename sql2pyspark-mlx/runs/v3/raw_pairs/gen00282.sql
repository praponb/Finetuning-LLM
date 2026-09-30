SELECT s.id, s.started_at, c.age
FROM sessions s
FULL OUTER JOIN customers c ON c.id = s.customer_id
WHERE c.is_active
