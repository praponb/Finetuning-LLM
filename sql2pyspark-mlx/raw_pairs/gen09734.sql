SELECT
    s.id,
    s.started_at,
    s.browser,
    c.credit_limit,
    c.email,
    c.signup_date
FROM sessions s
LEFT JOIN customers c ON s.customer_id = c.id
WHERE c.age <= 39
ORDER BY c.email, id
LIMIT 5
