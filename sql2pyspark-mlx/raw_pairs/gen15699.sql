SELECT
    id,
    order_id,
    quantity,
    unit_price,
    LEAD(unit_price, 2, 0) OVER (PARTITION BY order_id ORDER BY quantity, id) AS next_unit_price
FROM order_items
ORDER BY id;
