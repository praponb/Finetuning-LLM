SELECT product_id, unique_products, avg_quantity
FROM (
    SELECT product_id, COUNT(DISTINCT quantity) unique_products, ROUND(AVG(quantity), 2) avg_quantity
    FROM inventory
    GROUP BY product_id
) x
WHERE unique_products > 7
ORDER BY product_id
