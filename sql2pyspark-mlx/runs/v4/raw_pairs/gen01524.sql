SELECT c.id, c.age
FROM customers c
WHERE LENGTH(c.email) >= 13 AND EXISTS (SELECT 1 FROM sessions s WHERE s.customer_id = c.id AND TO_DATE(s.started_at) < '2024-10-12');
