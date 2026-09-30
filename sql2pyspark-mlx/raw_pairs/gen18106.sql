SELECT
    store_id,
    CASE WHEN quantity >= 239 THEN 'high' WHEN quantity >= 58 THEN 'mid' ELSE 'low' END quantity_bucket,
    product_id,
    COUNT(DISTINCT quantity) unique_stores,
    ROUND(AVG(quantity), 2) avg_quantity
FROM inventory
WHERE product_id IN (1, 6, 9, 10) OR last_restock > '2025-09-06'
GROUP BY 1, 2, 3
HAVING COUNT(DISTINCT quantity) < 4
ORDER BY store_id DESC
