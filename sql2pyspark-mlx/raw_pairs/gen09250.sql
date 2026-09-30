SELECT
    id,
    quantity,
    order_id,
    unit_price - quantity AS line_total
FROM order_items
WHERE quantity = 6
