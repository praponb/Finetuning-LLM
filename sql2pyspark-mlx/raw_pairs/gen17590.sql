SELECT
    s.id,
    s.plan,
    c.country,
    c.signup_date,
    c.credit_limit
FROM subscriptions s
INNER JOIN customers c ON c.id = s.customer_id
WHERE s.plan IN ('enterprise', 'basic');
