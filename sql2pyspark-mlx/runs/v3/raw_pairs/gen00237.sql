SELECT *
FROM (
    SELECT
        CASE WHEN YEAR(end_date) < 2024 THEN 'before_2024' WHEN YEAR(end_date) = 2024 THEN '2024' ELSE 'after_2024' END AS end_cohort,
        customer_id,
        ROUND((100 * SUM(CASE WHEN status = 'EXPIRED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_expired,
        MAX(monthly_fee) AS max_monthly_fee
    FROM subscriptions
    GROUP BY CASE WHEN YEAR(end_date) < 2024 THEN 'before_2024' WHEN YEAR(end_date) = 2024 THEN '2024' ELSE 'after_2024' END, customer_id
) AS sub
WHERE max_monthly_fee < 90
