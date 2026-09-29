SELECT
    id,
    quantity,
    product_id,
    ROUND(unit_price / quantity, 2) AS line_total
FROM order_items
WHERE unit_price >= 340;
