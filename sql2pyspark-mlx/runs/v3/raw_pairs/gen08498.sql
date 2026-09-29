SELECT
    id,
    unit_price,
    quantity,
    order_id,
    unit_price + quantity AS line_total
FROM order_items
WHERE order_id IN (2, 15, 20, 32);
