SELECT
    store_id,
    product_id,
    SUM(quantity) total_quantity,
    ROUND(AVG(quantity), 2) avg_quantity
FROM inventory
WHERE quantity <= 177 AND product_id IN (3, 5, 8)
GROUP BY store_id, product_id
ORDER BY avg_quantity DESC;
