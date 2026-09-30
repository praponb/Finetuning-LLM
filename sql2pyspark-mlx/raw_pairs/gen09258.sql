SELECT
    s.started_at,
    s.customer_id,
    c.signup_date,
    c.city
FROM sessions s
JOIN customers c ON s.customer_id = c.id
WHERE c.credit_limit > 5600 AND c.name IS NOT NULL;
