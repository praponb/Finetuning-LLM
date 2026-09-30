SELECT plan, MAX(monthly_fee) AS max_monthly_fee
FROM subscriptions
WHERE plan <> 'enterprise' AND (monthly_fee < 80 OR status NOT IN ('CANCELLED', 'ACTIVE', 'PAUSED'))
GROUP BY plan
HAVING MAX(monthly_fee) >= 110
ORDER BY plan DESC
LIMIT 20
