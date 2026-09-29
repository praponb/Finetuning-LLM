SELECT
    s.id,
    s.country,
    s.customer_id,
    c.is_active,
    c.age,
    c.signup_date
FROM sessions s
INNER JOIN customers c ON s.customer_id = c.id;
