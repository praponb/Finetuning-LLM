SELECT
    order_id,
    COUNT(DISTINCT unit_price) AS unique_orders,
    MAX(quantity) - MIN(quantity) AS quantity_range,
    MIN(unit_price) AS min_unit_price
FROM order_items
WHERE product_id = 36 AND product_id IN (16, 28, 33)
GROUP BY order_id
HAVING COUNT(unit_price) < 3;
