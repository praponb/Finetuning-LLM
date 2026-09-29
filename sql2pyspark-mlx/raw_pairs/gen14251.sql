SELECT
    id,
    product_id,
    AVG(quantity) OVER (PARTITION BY store_id, product_id) AS store_id_avg_quantity,
    NTILE(10) OVER (ORDER BY quantity DESC, id) AS quantity_bucket
FROM inventory;
