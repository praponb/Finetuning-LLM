SELECT
    s.started_at,
    s.country,
    c.credit_limit,
    c.name
FROM sessions s
LEFT JOIN customers c ON s.customer_id = c.id
WHERE c.name LIKE 'N%' OR LOWER(TRIM(c.email)) = 'bob.smith@yahoo.com';
