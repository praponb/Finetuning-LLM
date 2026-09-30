SELECT
    product_id,
    store_id,
    COUNT(DISTINCT quantity) AS unique_products,
    MAX(last_restock) AS last_last_restock
FROM inventory
GROUP BY product_id, store_id
HAVING MAX(quantity) > 170
