SELECT CASE WHEN YEAR(sale_date) < 2024 THEN 'before_2024' WHEN YEAR(sale_date) = 2024 THEN '2024' ELSE 'after_2024' END AS sale_cohort, MIN(sale_date) AS first_sale_date
FROM store_sales
WHERE product_id = 5
GROUP BY sale_cohort
ORDER BY sale_cohort
