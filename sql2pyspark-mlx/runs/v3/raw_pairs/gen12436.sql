SELECT
    id,
    store_id,
    quantity,
    AVG(quantity) OVER (PARTITION BY store_id) AS store_id_avg_quantity
FROM inventory
WHERE quantity = 99;
