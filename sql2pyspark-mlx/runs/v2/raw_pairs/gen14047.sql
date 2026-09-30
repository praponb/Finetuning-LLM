SELECT
    s.monthly_fee,
    s.plan,
    c.segment,
    c.email,
    c.credit_limit
FROM subscriptions s
INNER JOIN customers c ON s.customer_id = c.id;
