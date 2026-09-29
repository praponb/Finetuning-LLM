SELECT
    id,
    product_id,
    quantity,
    order_id,
    SUM(quantity) OVER (PARTITION BY product_id) AS product_id_sum_quantity,
    LEAD(quantity, 2) OVER (PARTITION BY order_id ORDER BY quantity, id) AS next_quantity
FROM order_items
WHERE quantity BETWEEN 4 AND 7
