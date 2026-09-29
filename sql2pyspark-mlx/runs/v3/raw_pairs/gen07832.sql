SELECT CASE WHEN monthly_fee >= 247 THEN 'high' WHEN monthly_fee >= 121 THEN 'mid' ELSE 'low' END AS monthly_fee_bucket, MIN(end_date) AS first_end_date
FROM subscriptions
GROUP BY monthly_fee_bucket
ORDER BY first_end_date
