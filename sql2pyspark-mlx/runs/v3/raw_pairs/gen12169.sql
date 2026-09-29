SELECT plan, COUNT(monthly_fee) AS monthly_fee_count
FROM subscriptions
GROUP BY plan
ORDER BY plan, monthly_fee_count DESC;
