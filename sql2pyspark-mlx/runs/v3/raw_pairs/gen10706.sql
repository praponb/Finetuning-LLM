SELECT
    plan,
    SUM(monthly_fee) AS total_monthly_fee,
    COUNT(*) AS cnt,
    SUM(SUM(monthly_fee)) OVER (ORDER BY plan) AS cumulative_monthly_fee
FROM subscriptions
WHERE end_date >= '2024-08-01'
GROUP BY plan
ORDER BY total_monthly_fee ASC, cumulative_monthly_fee ASC, plan ASC
LIMIT 10;
