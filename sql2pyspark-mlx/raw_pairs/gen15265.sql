SELECT
    s.id,
    s.end_date,
    s.plan,
    s.monthly_fee
FROM subscriptions s
WHERE s.status <> 'EXPIRED' AND s.customer_id IN (SELECT id FROM customers WHERE email NOT LIKE '%@gmail.com')
