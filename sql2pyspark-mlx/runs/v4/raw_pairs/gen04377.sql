SELECT plan, COUNT(monthly_fee) AS monthly_fee_count, AVG(monthly_fee) AS avg_monthly_fee
FROM subscriptions
WHERE status = 'EXPIRED'
GROUP BY plan
ORDER BY monthly_fee_count, plan;
