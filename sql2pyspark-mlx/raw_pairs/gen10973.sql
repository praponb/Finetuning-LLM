SELECT s.device, c.credit_limit, c.age
FROM sessions s
INNER JOIN customers c ON s.customer_id = c.id
ORDER BY age DESC;
