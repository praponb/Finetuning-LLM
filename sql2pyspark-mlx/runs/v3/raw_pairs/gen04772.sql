SELECT
    s.id,
    s.started_at,
    c.credit_limit,
    c.name,
    c.email
FROM sessions s
JOIN customers c ON s.customer_id = c.id
WHERE c.name LIKE 'Ol%'
ORDER BY c.email, s.started_at;
