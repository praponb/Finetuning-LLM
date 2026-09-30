SELECT
    order_id,
    product_id,
    COUNT(quantity) AS quantity_count,
    AVG(unit_price) AS avg_unit_price
FROM order_items
WHERE order_id IN (5, 9, 12, 15) OR quantity < 7
GROUP BY order_id, product_id;
