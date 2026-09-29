SELECT
    id,
    product_id,
    quantity,
    unit_price,
    SUM(unit_price) OVER (PARTITION BY product_id ORDER BY quantity, id) AS running_unit_price
FROM order_items
WHERE quantity = 6
ORDER BY quantity;
