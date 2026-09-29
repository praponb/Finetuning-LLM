SELECT CASE WHEN YEAR(opened_date) < 2024 THEN 'before_2024' WHEN YEAR(opened_date) = 2024 THEN '2024' ELSE 'after_2024' END AS opened_cohort, SUM(sqft) AS total_sqft, ROUND(SUM(sqft) * 100.0 / SUM(SUM(sqft)) OVER (), 2) AS pct_of_total
FROM stores
GROUP BY CASE WHEN YEAR(opened_date) < 2024 THEN 'before_2024' WHEN YEAR(opened_date) = 2024 THEN '2024' ELSE 'after_2024' END;
