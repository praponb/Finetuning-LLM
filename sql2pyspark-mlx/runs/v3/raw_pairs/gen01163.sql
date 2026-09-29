SELECT
    id,
    product_id,
    order_id,
    unit_price,
    ROUND(unit_price / quantity, 2) AS line_total,
    quantity - unit_price AS line_total_2
FROM order_items
WHERE product_id IN (11, 14) AND order_id IN (7, 17)
