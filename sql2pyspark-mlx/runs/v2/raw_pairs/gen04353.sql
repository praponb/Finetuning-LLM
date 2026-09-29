SELECT c.id, c.credit_limit
FROM customers c
WHERE EXISTS (SELECT 1 FROM sessions s WHERE s.customer_id = c.id)
ORDER BY c.credit_limit DESC;
