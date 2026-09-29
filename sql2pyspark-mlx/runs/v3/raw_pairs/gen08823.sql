SELECT product_id, CASE WHEN YEAR(last_restock) < 2024 THEN 'before_2024' WHEN YEAR(last_restock) = 2024 THEN '2024' ELSE 'after_2024' END AS last_restock_cohort, AVG(quantity) AS avg_quantity
FROM inventory
WHERE store_id IN (3, 4) AND (quantity <= 70 OR store_id IN (2, 3, 4, 6))
GROUP BY product_id, last_restock_cohort
