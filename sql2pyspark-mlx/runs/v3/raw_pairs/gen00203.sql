SELECT
    s.plan,
    s.monthly_fee,
    s.status,
    c.credit_limit,
    c.city
FROM subscriptions s
INNER JOIN customers c ON c.id = s.customer_id
WHERE c.country IN ('UK', 'DE') AND c.country = 'SG'
