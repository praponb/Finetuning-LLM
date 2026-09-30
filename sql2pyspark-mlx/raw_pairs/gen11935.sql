SELECT
    CASE WHEN YEAR(opened_date) < 2024 THEN 'before_2024' WHEN YEAR(opened_date) = 2024 THEN '2024' ELSE 'after_2024' END opened_cohort,
    city,
    MAX(sqft) - MIN(sqft) sqft_range,
    MIN(sqft) min_sqft
FROM stores
GROUP BY opened_cohort, city
ORDER BY city
