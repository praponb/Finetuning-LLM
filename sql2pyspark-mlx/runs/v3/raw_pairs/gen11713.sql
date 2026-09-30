SELECT
    id,
    product_id,
    quantity,
    order_id,
    FIRST_VALUE(order_id) OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS first_order_id,
    MAX(quantity) OVER (PARTITION BY order_id) AS order_id_max_quantity
FROM order_items
WHERE quantity <> 6
