SELECT CASE WHEN YEAR(shipped_date) < 2024 THEN 'before_2024' WHEN YEAR(shipped_date) = 2024 THEN '2024' ELSE 'after_2024' END shipped_cohort, MIN(delivered_date) first_delivered_date
FROM shipments
GROUP BY 1
