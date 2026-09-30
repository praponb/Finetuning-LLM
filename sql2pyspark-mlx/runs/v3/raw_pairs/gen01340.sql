SELECT
    id,
    order_id,
    quantity,
    LEAD(quantity, 1) OVER (PARTITION BY order_id ORDER BY quantity, id) AS next_quantity
FROM order_items
ORDER BY id
