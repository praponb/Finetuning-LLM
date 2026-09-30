SELECT
    id,
    product_id,
    quantity,
    AVG(quantity) OVER (PARTITION BY product_id) AS product_id_avg_quantity
FROM inventory
WHERE product_id IN (1, 2, 5, 8);
