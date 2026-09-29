SELECT
    opened_cohort,
    opened_month,
    total_count,
    first_opened_date
FROM (
    SELECT
        CASE WHEN YEAR(opened_date) < 2024 THEN 'before_2024' WHEN YEAR(opened_date) = 2024 THEN '2024' ELSE 'after_2024' END AS opened_cohort,
        DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month,
        COUNT(*) AS total_count,
        MIN(opened_date) AS first_opened_date
    FROM stores
    GROUP BY CASE WHEN YEAR(opened_date) < 2024 THEN 'before_2024' WHEN YEAR(opened_date) = 2024 THEN '2024' ELSE 'after_2024' END, DATE_FORMAT(opened_date, 'yyyy-MM')
) AS agg
WHERE total_count > 5
ORDER BY opened_month
