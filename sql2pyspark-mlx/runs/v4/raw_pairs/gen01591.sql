SELECT CASE WHEN monthly_fee >= 226 THEN 'high' WHEN monthly_fee >= 98 THEN 'mid' ELSE 'low' END AS monthly_fee_bucket, COUNT(DISTINCT monthly_fee) AS unique_statuss
FROM subscriptions
GROUP BY 1
ORDER BY unique_statuss DESC;
