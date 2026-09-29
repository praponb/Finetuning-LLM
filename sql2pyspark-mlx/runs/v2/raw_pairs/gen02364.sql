SELECT
    product_id,
    order_id,
    MIN(quantity) AS min_quantity,
    COUNT(*) AS row_count
FROM order_items
WHERE order_id IN (6, 28, 38, 40) AND (product_id = 8 OR order_id IN (29, 38, 39))
GROUP BY product_id, order_id
HAVING min_quantity < 8.0
ORDER BY product_id DESC
