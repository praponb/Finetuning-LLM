SELECT
    id,
    order_id,
    quantity,
    unit_price,
    product_id,
    LEAD(unit_price, 1) OVER (PARTITION BY order_id ORDER BY quantity, id) AS next_unit_price,
    NTILE(10) OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS bucket
FROM order_items
WHERE unit_price >= 230
