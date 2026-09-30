SELECT
    plan,
    DATE_TRUNC('MONTH', end_date) AS end_month,
    CASE WHEN YEAR(end_date) < 2024 THEN 'before_2024' WHEN YEAR(end_date) = 2024 THEN '2024' ELSE 'after_2024' END AS end_cohort,
    ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee,
    COUNT(*) AS cnt
FROM subscriptions
WHERE customer_id IS NOT NULL AND (monthly_fee BETWEEN 70 AND 130 OR end_date >= '2024-05-01')
GROUP BY plan, DATE_TRUNC('MONTH', end_date), CASE WHEN YEAR(end_date) < 2024 THEN 'before_2024' WHEN YEAR(end_date) = 2024 THEN '2024' ELSE 'after_2024' END
HAVING COUNT(*) < 3;
