SELECT CASE WHEN YEAR(sale_date) < 2024 THEN 'before_2024' WHEN YEAR(sale_date) = 2024 THEN '2024' ELSE 'after_2024' END AS sale_cohort, store_id, COUNT(units) AS units_count
FROM store_sales
WHERE units BETWEEN 15 AND 23 AND (store_id = 5 OR units IS NOT NULL)
GROUP BY CASE WHEN YEAR(sale_date) < 2024 THEN 'before_2024' WHEN YEAR(sale_date) = 2024 THEN '2024' ELSE 'after_2024' END, store_id
HAVING units_count >= 4;
