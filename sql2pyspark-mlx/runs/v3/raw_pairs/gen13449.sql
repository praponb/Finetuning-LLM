SELECT
    c.id,
    c.name,
    COUNT(s.id) AS subscription_count,
    MAX(s.monthly_fee) - MIN(s.monthly_fee) AS monthly_fee_range
FROM customers c
LEFT JOIN subscriptions s ON c.id = s.customer_id
WHERE s.status IN ('EXPIRED', 'PAUSED', 'CANCELLED')
GROUP BY c.id, c.name
ORDER BY id DESC, monthly_fee_range DESC
