SELECT
    order_id,
    MAX(unit_price) max_unit_price,
    MAX(quantity) max_quantity,
    MAX(quantity) - MIN(quantity) quantity_range
FROM order_items
GROUP BY order_id
HAVING SUM(unit_price * quantity) > 55;
