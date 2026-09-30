SELECT
    CASE WHEN YEAR(start_date) < 2024 THEN 'before_2024' WHEN YEAR(start_date) = 2024 THEN '2024' ELSE 'after_2024' END AS start_cohort,
    status,
    MAX(end_date) AS last_end_date,
    MAX(monthly_fee) AS max_monthly_fee
FROM subscriptions
GROUP BY CASE WHEN YEAR(start_date) < 2024 THEN 'before_2024' WHEN YEAR(start_date) = 2024 THEN '2024' ELSE 'after_2024' END, status;
