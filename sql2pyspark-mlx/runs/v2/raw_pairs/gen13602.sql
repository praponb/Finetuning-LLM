SELECT
    s.customer_id,
    s.started_at,
    c.is_active,
    c.signup_date,
    c.age
FROM sessions s
INNER JOIN customers c ON s.customer_id = c.id
WHERE c.credit_limit < 9800 AND s.customer_id = 6;
