SELECT
    CASE WHEN YEAR(last_restock) < 2024 THEN 'before_2024' WHEN YEAR(last_restock) = 2024 THEN '2024' ELSE 'after_2024' END AS last_restock_cohort,
    MIN(last_restock) AS first_last_restock,
    AVG(quantity) AS avg_quantity,
    COUNT(quantity) AS quantity_count
FROM inventory
WHERE quantity < 96 AND last_restock BETWEEN '2023-06-24' AND '2025-08-01' AND quantity <= 115
GROUP BY 1
HAVING AVG(quantity) > 150
