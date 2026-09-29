SELECT CASE WHEN monthly_fee >= 267 THEN 'high' WHEN monthly_fee >= 60 THEN 'mid' ELSE 'low' END AS monthly_fee_bucket, SUM(monthly_fee) AS total_monthly_fee, ROUND(SUM(monthly_fee) * 100.0 / SUM(SUM(monthly_fee)) OVER (), 2) AS pct_of_total
FROM subscriptions
WHERE start_date <= '2024-03-01'
GROUP BY CASE WHEN monthly_fee >= 267 THEN 'high' WHEN monthly_fee >= 60 THEN 'mid' ELSE 'low' END
ORDER BY monthly_fee_bucket, total_monthly_fee DESC
