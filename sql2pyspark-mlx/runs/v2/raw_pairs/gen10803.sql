SELECT
    product_id,
    CASE WHEN quantity >= 229 THEN 'high' WHEN quantity >= 114 THEN 'mid' ELSE 'low' END AS quantity_bucket,
    MAX(quantity) - MIN(quantity) AS quantity_range,
    SUM(quantity) AS total_quantity,
    ROUND(AVG(quantity), 2) AS avg_quantity
FROM inventory
GROUP BY product_id, CASE WHEN quantity >= 229 THEN 'high' WHEN quantity >= 114 THEN 'mid' ELSE 'low' END
HAVING SUM(quantity) >= 990
ORDER BY quantity_range DESC, product_id, quantity_bucket
LIMIT 20
