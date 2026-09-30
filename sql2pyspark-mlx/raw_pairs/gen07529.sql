SELECT CASE WHEN YEAR(last_restock) < 2024 THEN 'before_2024' WHEN YEAR(last_restock) = 2024 THEN '2024' ELSE 'after_2024' END AS last_restock_cohort, MIN(last_restock) AS min_last_restock, SUM(quantity) AS total_quantity
FROM inventory
WHERE store_id IN (2, 3) AND (quantity <= 82 OR product_id = 1)
GROUP BY 1;
