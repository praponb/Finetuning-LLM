SELECT
    s.id,
    s.customer_id,
    c.country,
    c.is_active
FROM sessions s
INNER JOIN customers c ON s.customer_id = c.id
ORDER BY s.customer_id ASC, c.country ASC
