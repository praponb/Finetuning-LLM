SELECT
    id,
    product_id,
    quantity,
    SUM(quantity) OVER (PARTITION BY product_id) AS product_id_sum_quantity
FROM inventory
WHERE store_id IN (3, 4, 6)
