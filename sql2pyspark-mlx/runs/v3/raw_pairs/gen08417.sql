SELECT plan, SUM(monthly_fee) AS total_monthly_fee, DENSE_RANK() OVER (ORDER BY SUM(monthly_fee) DESC) AS monthly_fee_rank
FROM subscriptions
GROUP BY plan
ORDER BY plan DESC, monthly_fee_rank DESC
LIMIT 50
