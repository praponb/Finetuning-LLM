SELECT
    YEAR(end_date) AS end_year,
    plan,
    AVG(monthly_fee) AS avg_monthly_fee,
    COUNT(*) AS row_count
FROM subscriptions
GROUP BY CUBE(YEAR(end_date), plan)
ORDER BY plan
