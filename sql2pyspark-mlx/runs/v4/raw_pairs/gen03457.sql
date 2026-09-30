SELECT
    id,
    product_id,
    quantity,
    SUM(quantity) OVER (PARTITION BY product_id) AS product_id_sum_quantity
FROM inventory
ORDER BY id DESC;
