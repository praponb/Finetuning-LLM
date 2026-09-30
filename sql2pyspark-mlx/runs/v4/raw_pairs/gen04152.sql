SELECT
    c.is_active,
    c.city,
    s.monthly_fee,
    s.plan
FROM customers c
LEFT JOIN subscriptions s ON c.id = s.customer_id
WHERE s.plan = 'basic'
ORDER BY s.monthly_fee ASC, s.plan ASC;
