SELECT CASE WHEN monthly_fee >= 236 THEN 'high' WHEN monthly_fee >= 82 THEN 'mid' ELSE 'low' END AS monthly_fee_bucket, status, MAX(monthly_fee) AS max_monthly_fee
FROM subscriptions
GROUP BY CUBE(CASE WHEN monthly_fee >= 236 THEN 'high' WHEN monthly_fee >= 82 THEN 'mid' ELSE 'low' END, status)
ORDER BY status;
