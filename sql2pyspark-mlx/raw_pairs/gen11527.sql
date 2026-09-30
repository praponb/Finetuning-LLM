SELECT product_id, CASE WHEN YEAR(sale_date) < 2024 THEN 'before_2024' WHEN YEAR(sale_date) = 2024 THEN '2024' ELSE 'after_2024' END AS sale_cohort, MAX(units) AS max_units
FROM store_sales
GROUP BY 1, 2
HAVING MAX(units) >= 39
ORDER BY max_units DESC;
