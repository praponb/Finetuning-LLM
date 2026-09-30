SELECT
    s.id,
    s.monthly_fee,
    c.city,
    c.country,
    c.signup_date
FROM subscriptions s
LEFT JOIN customers c ON s.customer_id = c.id
ORDER BY country, id DESC;
