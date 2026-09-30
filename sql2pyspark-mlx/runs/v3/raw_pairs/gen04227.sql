SELECT
    plan,
    COUNT(*) AS total_count,
    MIN(monthly_fee) AS min_monthly_fee,
    MIN(end_date) AS first_end_date
FROM subscriptions
GROUP BY plan
ORDER BY first_end_date
