SELECT
    id,
    quantity,
    unit_price,
    LEAD(unit_price) OVER (PARTITION BY order_id, product_id ORDER BY quantity, id) AS next_unit_price,
    LAG(quantity) OVER (PARTITION BY order_id, product_id ORDER BY quantity, id) AS prev_quantity,
    LEAD(quantity) OVER (PARTITION BY order_id, product_id ORDER BY quantity, id) AS next_quantity,
    ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY quantity, id) AS row_seq
FROM order_items
