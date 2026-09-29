SELECT
    id,
    order_id,
    unit_price,
    quantity,
    LAST_VALUE(quantity) OVER (PARTITION BY order_id ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_quantity
FROM order_items;
