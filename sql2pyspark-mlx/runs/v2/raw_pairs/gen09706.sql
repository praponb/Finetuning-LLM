SELECT
    c.email,
    c.age,
    c.signup_date,
    s.country,
    s.started_at
FROM customers c
LEFT OUTER JOIN sessions s ON c.id = s.customer_id
