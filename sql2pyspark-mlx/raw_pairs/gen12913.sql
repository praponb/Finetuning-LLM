SELECT
    CASE WHEN quantity >= 246 THEN 'high' WHEN quantity >= 47 THEN 'mid' ELSE 'low' END AS quantity_bucket,
    store_id,
    MAX(quantity) - MIN(quantity) AS quantity_range,
    MIN(last_restock) AS first_last_restock,
    COUNT(quantity) AS quantity_count
FROM inventory
GROUP BY 1, 2
ORDER BY quantity_bucket DESC
