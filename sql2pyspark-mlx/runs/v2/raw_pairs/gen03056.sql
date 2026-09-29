SELECT
    s.customer_id,
    c.email,
    c.name,
    c.age
FROM sessions s
INNER JOIN customers c ON s.customer_id = c.id
WHERE c.city IN ('London', 'Singapore');
