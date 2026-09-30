SELECT
    CASE WHEN YEAR(last_restock) < 2024 THEN 'before_2024' WHEN YEAR(last_restock) = 2024 THEN '2024' ELSE 'after_2024' END AS last_restock_cohort,
    CASE WHEN quantity >= 261 THEN 'high' WHEN quantity >= 119 THEN 'mid' ELSE 'low' END AS quantity_bucket,
    MAX(quantity) - MIN(quantity) AS quantity_range,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    COUNT(DISTINCT quantity) AS unique_stores
FROM inventory
GROUP BY last_restock_cohort, quantity_bucket
HAVING MAX(quantity) - MIN(quantity) >= 47
ORDER BY quantity_bucket;
