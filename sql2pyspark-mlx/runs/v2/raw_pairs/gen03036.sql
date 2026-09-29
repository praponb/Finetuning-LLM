SELECT
    s.status,
    s.plan,
    s.start_date,
    c.signup_date,
    c.credit_limit
FROM subscriptions s
JOIN customers c ON s.customer_id = c.id
ORDER BY plan, credit_limit DESC;
