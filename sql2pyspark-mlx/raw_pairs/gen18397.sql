SELECT
    plan,
    CASE WHEN monthly_fee >= 261 THEN 'high' WHEN monthly_fee >= 48 THEN 'mid' ELSE 'low' END AS monthly_fee_bucket,
    MIN(monthly_fee) AS min_monthly_fee,
    MIN(start_date) AS first_start_date
FROM subscriptions
WHERE YEAR(start_date) >= 2024
GROUP BY 1, 2
ORDER BY monthly_fee_bucket ASC, plan ASC
LIMIT 20
