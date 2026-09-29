SELECT
    CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END quantity_bucket,
    order_id,
    COUNT(DISTINCT unit_price) unique_products,
    SUM(quantity) total_quantity
FROM order_items
WHERE order_id IN (8, 24, 38) AND quantity <> 5
GROUP BY CUBE(CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END, order_id)
