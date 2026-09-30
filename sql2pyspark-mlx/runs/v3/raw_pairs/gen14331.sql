SELECT
    CASE WHEN monthly_fee >= 239 THEN 'high' WHEN monthly_fee >= 50 THEN 'mid' ELSE 'low' END AS monthly_fee_bucket,
    SUM(monthly_fee) AS total_monthly_fee,
    COUNT(*) AS cnt,
    RANK() OVER (ORDER BY SUM(monthly_fee) DESC) AS rnk
FROM subscriptions
GROUP BY CASE WHEN monthly_fee >= 239 THEN 'high' WHEN monthly_fee >= 50 THEN 'mid' ELSE 'low' END
ORDER BY total_monthly_fee DESC, monthly_fee_bucket
LIMIT 20
