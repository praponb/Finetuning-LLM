SELECT CASE WHEN YEAR(sale_date) < 2024 THEN 'before_2024' WHEN YEAR(sale_date) = 2024 THEN '2024' ELSE 'after_2024' END AS sale_cohort, YEAR(sale_date) AS sale_year, AVG(revenue) AS avg_revenue
FROM store_sales
WHERE store_id IN (2, 4)
GROUP BY 1, 2
ORDER BY avg_revenue DESC, sale_cohort, sale_year
LIMIT 50
