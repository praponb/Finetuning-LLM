SELECT
    id,
    product_id,
    order_id,
    quantity,
    LAG(quantity, 1) OVER (PARTITION BY product_id, order_id ORDER BY quantity, id) AS prev_quantity,
    MAX(quantity) OVER (PARTITION BY product_id) AS product_id_max_quantity
FROM order_items
ORDER BY id DESC, product_id DESC
