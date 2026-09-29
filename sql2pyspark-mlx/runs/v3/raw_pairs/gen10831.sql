SELECT
    product_id,
    CASE WHEN YEAR(last_restock) < 2024 THEN 'before_2024' WHEN YEAR(last_restock) = 2024 THEN '2024' ELSE 'after_2024' END AS last_restock_cohort,
    CASE WHEN quantity >= 261 THEN 'high' WHEN quantity >= 70 THEN 'mid' ELSE 'low' END AS quantity_bucket,
    ROUND(STDDEV(quantity), 2) AS stddev_quantity,
    MIN(last_restock) AS first_last_restock,
    MAX(quantity) AS max_quantity
FROM inventory
GROUP BY product_id, CASE WHEN YEAR(last_restock) < 2024 THEN 'before_2024' WHEN YEAR(last_restock) = 2024 THEN '2024' ELSE 'after_2024' END, CASE WHEN quantity >= 261 THEN 'high' WHEN quantity >= 70 THEN 'mid' ELSE 'low' END
