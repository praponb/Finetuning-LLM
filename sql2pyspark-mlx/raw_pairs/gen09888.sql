SELECT CASE WHEN YEAR(last_restock) < 2024 THEN 'before_2024' WHEN YEAR(last_restock) = 2024 THEN '2024' ELSE 'after_2024' END AS last_restock_cohort, store_id, MIN(quantity) AS min_quantity
FROM inventory
WHERE last_restock > '2025-10-01'
GROUP BY 1, 2;
