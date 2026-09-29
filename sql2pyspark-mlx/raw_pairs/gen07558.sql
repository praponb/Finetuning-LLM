SELECT
    s.id,
    s.started_at,
    s.country,
    s.customer_id,
    c.signup_date
FROM sessions s
INNER JOIN customers c ON s.customer_id = c.id
WHERE s.customer_id IS NOT NULL
