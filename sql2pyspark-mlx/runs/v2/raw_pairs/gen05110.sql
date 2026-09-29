SELECT
    id,
    order_id,
    unit_price,
    quantity,
    LEAD(quantity, 1, 0) OVER (PARTITION BY order_id ORDER BY unit_price, id) AS next_quantity
FROM order_items
