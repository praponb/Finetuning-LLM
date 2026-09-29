WITH base AS (
    SELECT *
    FROM stores
    WHERE region IN ('Central', 'West')
),
summary AS (
    SELECT
        CASE WHEN YEAR(opened_date) < 2024 THEN 'before_2024' WHEN YEAR(opened_date) = 2024 THEN '2024' ELSE 'after_2024' END opened_cohort,
        YEAR(opened_date) opened_year,
        COUNT(*) row_count,
        SUM(CASE WHEN city = 'London' THEN 1 ELSE 0 END) london_count
    FROM base
    GROUP BY CASE WHEN YEAR(opened_date) < 2024 THEN 'before_2024' WHEN YEAR(opened_date) = 2024 THEN '2024' ELSE 'after_2024' END, YEAR(opened_date)
)
SELECT *
FROM summary
WHERE row_count > 4
ORDER BY london_count DESC
