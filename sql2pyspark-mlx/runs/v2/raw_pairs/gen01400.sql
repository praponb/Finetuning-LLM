SELECT
    s.id,
    s.started_at,
    s.country,
    c.age,
    c.segment,
    c.signup_date
FROM sessions s
INNER JOIN customers c ON s.customer_id = c.id
WHERE NOT c.is_active
ORDER BY age DESC, s.started_at ASC;
