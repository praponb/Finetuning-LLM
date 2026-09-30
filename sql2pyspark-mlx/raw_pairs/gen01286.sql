SELECT plan, SUM(monthly_fee) AS total_monthly_fee, DENSE_RANK() OVER (ORDER BY SUM(monthly_fee) DESC) AS rnk
FROM subscriptions
GROUP BY plan
ORDER BY total_monthly_fee DESC
