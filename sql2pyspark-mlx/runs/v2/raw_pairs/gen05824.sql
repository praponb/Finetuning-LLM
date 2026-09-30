SELECT
    product_id,
    store_id,
    MAX(last_restock) AS latest_last_restock,
    MAX(quantity) - MIN(quantity) AS quantity_range,
    COUNT(DISTINCT quantity) AS unique_products
FROM inventory
WHERE store_id IN (1, 3, 4) AND store_id IN (3, 4, 5, 6) AND YEAR(last_restock) = 2025
GROUP BY product_id, store_id
HAVING COUNT(DISTINCT quantity) >= 2
ORDER BY latest_last_restock, store_id;
