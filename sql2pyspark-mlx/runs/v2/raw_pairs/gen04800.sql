SELECT
    id,
    store_id,
    quantity,
    product_id,
    MAX(quantity) OVER (PARTITION BY store_id) AS store_id_max_quantity,
    AVG(quantity) OVER (PARTITION BY product_id) AS product_id_avg_quantity
FROM inventory
WHERE store_id IN (1, 2, 4, 5);
