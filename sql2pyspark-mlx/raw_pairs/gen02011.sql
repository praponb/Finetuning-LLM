SELECT
    id,
    product_id,
    order_id,
    quantity,
    unit_price,
    LAST_VALUE(unit_price) OVER (PARTITION BY product_id, order_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_unit_price
FROM order_items
