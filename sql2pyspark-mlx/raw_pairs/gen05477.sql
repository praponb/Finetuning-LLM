SELECT
    product_id,
    CASE WHEN YEAR(last_restock) < 2024 THEN 'before_2024' WHEN YEAR(last_restock) = 2024 THEN '2024' ELSE 'after_2024' END AS last_restock_cohort,
    MIN(last_restock) AS min_last_restock,
    MIN(quantity) AS min_quantity
FROM inventory
WHERE last_restock > '2025-05-01'
GROUP BY product_id, last_restock_cohort
HAVING COUNT(*) < 5;
