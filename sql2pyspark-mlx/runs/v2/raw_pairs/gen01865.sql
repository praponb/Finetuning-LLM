SELECT
    c.id,
    c.segment,
    c.is_active,
    c.name
FROM customers c
WHERE EXISTS (SELECT 1 FROM sessions s WHERE s.customer_id = c.id AND s.customer_id = 19)
ORDER BY c.segment DESC
