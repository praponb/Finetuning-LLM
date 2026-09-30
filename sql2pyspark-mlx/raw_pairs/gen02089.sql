SELECT
    c.country,
    COUNT(s.id) AS subscription_count,
    MAX(s.monthly_fee) - MIN(s.monthly_fee) AS monthly_fee_range,
    SUM(CASE WHEN s.status = 'PAUSED' THEN s.monthly_fee ELSE 0 END) AS paused_monthly_fee
FROM customers c
LEFT OUTER JOIN subscriptions s ON c.id = s.customer_id
WHERE s.customer_id IS NOT NULL
GROUP BY c.country
ORDER BY monthly_fee_range DESC, country
LIMIT 5
