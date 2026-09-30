SELECT *
FROM (
    SELECT
        DATE_TRUNC('MONTH', opened_date) AS opened_month,
        CASE WHEN YEAR(opened_date) < 2024 THEN 'before_2024' WHEN YEAR(opened_date) = 2024 THEN '2024' ELSE 'after_2024' END AS opened_cohort,
        SORT_ARRAY(COLLECT_SET(region)) AS region_list,
        SUM(sqft) AS total_sqft
    FROM stores
    WHERE opened_date < '2024-09-01'
    GROUP BY DATE_TRUNC('MONTH', opened_date), CASE WHEN YEAR(opened_date) < 2024 THEN 'before_2024' WHEN YEAR(opened_date) = 2024 THEN '2024' ELSE 'after_2024' END
) AS s
WHERE total_sqft < 37000
ORDER BY total_sqft, opened_month, opened_cohort
LIMIT 10
