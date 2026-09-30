SELECT
    s.started_at,
    s.country,
    c.signup_date,
    c.age
FROM sessions s
INNER JOIN customers c ON c.id = s.customer_id
WHERE s.customer_id = 22 AND c.credit_limit > 11300
ORDER BY age
