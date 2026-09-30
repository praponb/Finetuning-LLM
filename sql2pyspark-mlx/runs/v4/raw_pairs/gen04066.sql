SELECT CASE WHEN YEAR(last_restock) < 2024 THEN 'before_2024' WHEN YEAR(last_restock) = 2024 THEN '2024' ELSE 'after_2024' END last_restock_cohort, ROUND(AVG(quantity), 2) avg_quantity
FROM inventory
WHERE last_restock <= '2025-02-01' AND (last_restock BETWEEN '2023-03-01' AND '2025-04-01' OR store_id IN (1, 4, 5))
GROUP BY last_restock_cohort
ORDER BY last_restock_cohort DESC
