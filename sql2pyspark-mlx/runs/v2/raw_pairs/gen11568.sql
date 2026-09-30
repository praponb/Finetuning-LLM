SELECT
    s.browser,
    s.country,
    c.segment,
    c.email,
    c.signup_date
FROM sessions s
LEFT JOIN customers c ON s.customer_id = c.id
ORDER BY email;
