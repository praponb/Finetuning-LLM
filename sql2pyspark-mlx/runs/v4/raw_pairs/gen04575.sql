SELECT *
FROM (
    SELECT
        plan,
        CASE WHEN YEAR(end_date) < 2024 THEN 'before_2024' WHEN YEAR(end_date) = 2024 THEN '2024' ELSE 'after_2024' END AS end_cohort,
        COUNT(*) AS cnt,
        MAX(monthly_fee) - MIN(monthly_fee) AS monthly_fee_range
    FROM subscriptions
    GROUP BY plan, CASE WHEN YEAR(end_date) < 2024 THEN 'before_2024' WHEN YEAR(end_date) = 2024 THEN '2024' ELSE 'after_2024' END
) AS sub
WHERE cnt < 10
ORDER BY monthly_fee_range, plan, end_cohort
LIMIT 20
