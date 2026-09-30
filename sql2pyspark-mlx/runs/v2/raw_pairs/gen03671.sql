SELECT
    s.id,
    s.country,
    c.city,
    c.is_active,
    c.credit_limit
FROM sessions s
LEFT JOIN customers c ON c.id = s.customer_id
ORDER BY s.country ASC
