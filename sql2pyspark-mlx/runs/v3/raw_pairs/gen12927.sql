SELECT opened_cohort, region, west_sqft
FROM (
    SELECT CASE WHEN YEAR(opened_date) < 2024 THEN 'before_2024' WHEN YEAR(opened_date) = 2024 THEN '2024' ELSE 'after_2024' END AS opened_cohort, region, SUM(CASE WHEN region = 'West' THEN sqft ELSE 0 END) AS west_sqft
    FROM stores
    GROUP BY CASE WHEN YEAR(opened_date) < 2024 THEN 'before_2024' WHEN YEAR(opened_date) = 2024 THEN '2024' ELSE 'after_2024' END, region
) AS agg
WHERE west_sqft >= 26
