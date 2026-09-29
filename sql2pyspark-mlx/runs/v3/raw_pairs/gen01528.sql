SELECT
    product_id,
    AVG(quantity) AS avg_quantity,
    ROUND(AVG(unit_price), 2) AS avg_unit_price,
    MIN(quantity) AS min_quantity
FROM order_items
WHERE product_id IS NULL AND order_id IN (20, 23) AND product_id IN (23, 37)
GROUP BY product_id;
