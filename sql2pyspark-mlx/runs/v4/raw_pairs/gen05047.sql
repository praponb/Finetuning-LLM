SELECT
    s.id,
    s.monthly_fee,
    s.start_date,
    c.signup_date
FROM subscriptions s
INNER JOIN customers c ON c.id = s.customer_id
WHERE s.plan = 'enterprise' OR c.city IN ('Berlin', 'Bangkok')
