SELECT
    s.id,
    s.monthly_fee,
    s.plan,
    s.status,
    c.credit_limit,
    c.email,
    c.name
FROM subscriptions s
LEFT OUTER JOIN customers c ON s.customer_id = c.id;
