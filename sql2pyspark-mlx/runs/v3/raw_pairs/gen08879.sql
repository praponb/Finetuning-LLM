SELECT
    id,
    product_id,
    quantity,
    order_id,
    FIRST_VALUE(order_id) OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS first_order_id
FROM order_items;
