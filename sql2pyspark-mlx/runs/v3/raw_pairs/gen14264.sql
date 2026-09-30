SELECT plan, status, MAX(monthly_fee) - MIN(monthly_fee) AS monthly_fee_range
FROM subscriptions
WHERE plan IN ('enterprise', 'standard', 'premium')
GROUP BY plan, status
HAVING MAX(monthly_fee) - MIN(monthly_fee) >= 64
