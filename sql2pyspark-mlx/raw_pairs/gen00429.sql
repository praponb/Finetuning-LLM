SELECT
    c.country,
    COUNT(s.id) AS subscription_count,
    AVG(s.monthly_fee) AS avg_monthly_fee,
    COUNT(s.monthly_fee) AS monthly_fee_count
FROM customers c
LEFT JOIN subscriptions s ON s.customer_id = c.id
GROUP BY c.country
ORDER BY c.country
LIMIT 20;
