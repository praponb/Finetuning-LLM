SELECT *
FROM (
    SELECT CASE WHEN YEAR(launch_date) < 2024 THEN 'before_2024' WHEN YEAR(launch_date) = 2024 THEN '2024' ELSE 'after_2024' END AS launch_cohort, COUNT(cost) AS cost_count
    FROM products
    GROUP BY CASE WHEN YEAR(launch_date) < 2024 THEN 'before_2024' WHEN YEAR(launch_date) = 2024 THEN '2024' ELSE 'after_2024' END
) AS x
WHERE cost_count > 8
ORDER BY launch_cohort DESC
LIMIT 3
