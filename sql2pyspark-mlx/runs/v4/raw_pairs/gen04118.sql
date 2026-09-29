SELECT
    product_id,
    COUNT(DISTINCT quantity) AS unique_products,
    SUM(quantity) AS total_quantity,
    COUNT(quantity) AS quantity_count
FROM inventory
GROUP BY product_id
ORDER BY unique_products, product_id
LIMIT 5;
