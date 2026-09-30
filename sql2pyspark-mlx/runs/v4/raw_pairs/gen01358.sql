SELECT
    CASE WHEN YEAR(last_restock) < 2024 THEN 'before_2024' WHEN YEAR(last_restock) = 2024 THEN '2024' ELSE 'after_2024' END last_restock_cohort,
    store_id,
    MIN(last_restock) min_last_restock,
    MIN(last_restock) first_last_restock
FROM inventory
WHERE quantity BETWEEN 84 AND 150 OR store_id IN (2, 3, 4, 5)
GROUP BY ROLLUP(CASE WHEN YEAR(last_restock) < 2024 THEN 'before_2024' WHEN YEAR(last_restock) = 2024 THEN '2024' ELSE 'after_2024' END, store_id)
