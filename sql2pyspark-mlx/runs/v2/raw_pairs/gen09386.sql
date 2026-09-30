SELECT
    id,
    quantity,
    order_id,
    ROUND(unit_price / quantity, 2) AS line_total
FROM order_items
WHERE product_id IS NULL AND unit_price BETWEEN 160 AND 360 AND quantity BETWEEN 4 AND 5;
