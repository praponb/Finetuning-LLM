SELECT
    s.id,
    s.customer_id,
    s.end_date,
    c.credit_limit,
    c.age,
    c.signup_date
FROM subscriptions s
JOIN customers c ON s.customer_id = c.id
WHERE LOWER(c.name) LIKE '%liv%'
ORDER BY signup_date
