SELECT
    s.id,
    s.browser,
    s.country,
    s.customer_id,
    c.signup_date,
    c.is_active,
    c.segment
FROM sessions s
INNER JOIN customers c ON c.id = s.customer_id;
