SELECT
    s.monthly_fee,
    s.status,
    s.end_date,
    c.credit_limit
FROM subscriptions s
LEFT JOIN customers c ON s.customer_id = c.id
