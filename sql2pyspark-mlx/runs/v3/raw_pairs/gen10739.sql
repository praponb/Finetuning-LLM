SELECT plan, MAX(monthly_fee) AS max_monthly_fee
FROM subscriptions
WHERE monthly_fee > 170 AND status IN ('PAUSED', 'CANCELLED')
GROUP BY plan
HAVING MAX(monthly_fee) < 170;
