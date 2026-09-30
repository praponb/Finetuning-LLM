SELECT
    id,
    product_id,
    quantity,
    unit_price,
    LAST_VALUE(unit_price) OVER (PARTITION BY product_id ORDER BY quantity DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_unit_price,
    LEAD(unit_price, 1) OVER (ORDER BY quantity, id) AS next_unit_price
FROM order_items
