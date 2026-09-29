SELECT
    DATE_FORMAT(last_restock, 'yyyy-MM') AS last_restock_month,
    CASE WHEN YEAR(last_restock) < 2024 THEN 'before_2024' WHEN YEAR(last_restock) = 2024 THEN '2024' ELSE 'after_2024' END AS last_restock_cohort,
    MAX(last_restock) AS latest_last_restock,
    MAX(last_restock) AS last_last_restock,
    MAX(quantity) - MIN(quantity) AS quantity_range
FROM inventory
WHERE product_id IN (2, 10) AND product_id IN (1, 5, 7)
GROUP BY last_restock_month, last_restock_cohort
ORDER BY last_last_restock;
