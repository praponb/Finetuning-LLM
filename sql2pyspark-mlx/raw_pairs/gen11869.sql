SELECT
    id,
    order_id,
    quantity,
    unit_price + quantity AS line_total
FROM order_items
WHERE quantity != 3
ORDER BY quantity DESC;
