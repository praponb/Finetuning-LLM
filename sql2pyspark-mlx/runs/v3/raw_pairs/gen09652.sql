SELECT
    s.monthly_fee,
    s.plan,
    s.end_date,
    c.age,
    c.signup_date,
    c.email
FROM subscriptions s
JOIN customers c ON s.customer_id = c.id
WHERE s.plan IN ('basic', 'enterprise') AND s.plan = 'standard';
