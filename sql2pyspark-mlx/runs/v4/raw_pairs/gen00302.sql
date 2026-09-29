SELECT *
FROM (
    SELECT city, CASE WHEN YEAR(opened_date) < 2024 THEN 'before_2024' WHEN YEAR(opened_date) = 2024 THEN '2024' ELSE 'after_2024' END AS opened_cohort, MAX(sqft) - MIN(sqft) AS sqft_range
    FROM stores
    GROUP BY city, CASE WHEN YEAR(opened_date) < 2024 THEN 'before_2024' WHEN YEAR(opened_date) = 2024 THEN '2024' ELSE 'after_2024' END
) AS s
WHERE sqft_range > 64;
