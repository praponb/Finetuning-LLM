SELECT
    id,
    product_id,
    quantity,
    MIN(quantity) OVER (PARTITION BY product_id) product_id_min_quantity
FROM order_items
ORDER BY id, product_id_min_quantity
