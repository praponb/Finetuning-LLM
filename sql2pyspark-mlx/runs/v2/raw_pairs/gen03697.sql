SELECT
    product_id,
    MAX(last_restock) AS last_last_restock,
    MAX(last_restock) AS latest_last_restock,
    COUNT(DISTINCT quantity) AS unique_products
FROM inventory
WHERE quantity > 124 AND (quantity IS NOT NULL OR product_id IN (1, 3, 6, 8))
GROUP BY product_id;
