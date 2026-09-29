WITH src AS (
    SELECT *, ROUND(cost, 0) AS cost_rounded
    FROM products
    WHERE SIZE(tags) > 2
),
summary AS (
    SELECT CASE WHEN YEAR(launch_date) < 2024 THEN 'before_2024' WHEN YEAR(launch_date) = 2024 THEN '2024' ELSE 'after_2024' END AS launch_cohort, MAX(launch_date) AS last_launch_date, COUNT(*) AS cnt
    FROM src
    GROUP BY CASE WHEN YEAR(launch_date) < 2024 THEN 'before_2024' WHEN YEAR(launch_date) = 2024 THEN '2024' ELSE 'after_2024' END
)
SELECT *
FROM summary
WHERE cnt > 7
ORDER BY cnt ASC
