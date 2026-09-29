SELECT
    id,
    order_id,
    quantity,
    MIN(quantity) OVER (PARTITION BY order_id) AS order_id_min_quantity
FROM order_items;
