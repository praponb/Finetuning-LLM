SELECT
    id,
    product_id,
    unit_price,
    quantity,
    quantity + unit_price AS line_total,
    ROUND(quantity, 0) AS quantity_rounded
FROM order_items
WHERE (order_id = 13 AND unit_price BETWEEN 240 AND 250) OR quantity <> 4
