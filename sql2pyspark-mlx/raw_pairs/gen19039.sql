SELECT
    CASE WHEN YEAR(last_restock) < 2024 THEN 'before_2024' WHEN YEAR(last_restock) = 2024 THEN '2024' ELSE 'after_2024' END AS last_restock_cohort,
    DATE_TRUNC('MONTH', last_restock) AS last_restock_month,
    ROUND(STDDEV(quantity), 2) AS stddev_quantity,
    COUNT(quantity) AS quantity_count,
    MAX(last_restock) AS latest_last_restock
FROM inventory
GROUP BY 1, 2
ORDER BY quantity_count DESC, last_restock_cohort, last_restock_month
LIMIT 20
