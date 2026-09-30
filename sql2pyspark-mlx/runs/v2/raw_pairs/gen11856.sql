SELECT
    s.device,
    s.country,
    s.browser,
    c.country AS customer_country,
    c.signup_date,
    c.city
FROM sessions s
RIGHT JOIN customers c ON s.customer_id = c.id
