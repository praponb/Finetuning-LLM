SELECT
    s.id,
    s.start_date,
    c.is_active,
    c.signup_date
FROM subscriptions s
LEFT OUTER JOIN customers c ON s.customer_id = c.id
WHERE c.is_active
ORDER BY start_date;
