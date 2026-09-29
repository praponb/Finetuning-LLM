SELECT
    product_id,
    QUARTER(last_restock) AS last_restock_quarter,
    CASE WHEN YEAR(last_restock) < 2024 THEN 'before_2024' WHEN YEAR(last_restock) = 2024 THEN '2024' ELSE 'after_2024' END AS last_restock_cohort,
    MAX(last_restock) AS last_last_restock,
    MAX(quantity) - MIN(quantity) AS quantity_range
FROM inventory
GROUP BY product_id, last_restock_quarter, last_restock_cohort
HAVING COUNT(*) > 2
