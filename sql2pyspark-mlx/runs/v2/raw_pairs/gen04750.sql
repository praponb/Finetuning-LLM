SELECT status, SUM(monthly_fee) AS total_monthly_fee, ROUND(SUM(monthly_fee) * 100.0 / SUM(SUM(monthly_fee)) OVER (), 2) AS pct_of_total
FROM subscriptions
WHERE customer_id = 17
GROUP BY status
ORDER BY status DESC
