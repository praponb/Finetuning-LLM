SELECT
    product_id,
    store_id,
    MAX(quantity) AS max_quantity,
    COUNT(DISTINCT quantity) AS unique_products
FROM inventory
GROUP BY product_id, store_id
HAVING max_quantity > 120;
