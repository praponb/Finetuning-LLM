SELECT
    order_id,
    product_id,
    SUM(unit_price * quantity) revenue,
    MAX(quantity) - MIN(quantity) quantity_range
FROM order_items
WHERE quantity BETWEEN 7 AND 8
GROUP BY order_id, product_id
HAVING SUM(unit_price * quantity) < 72
ORDER BY quantity_range DESC, order_id
