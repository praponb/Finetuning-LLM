SELECT
    id,
    store_id,
    product_id,
    quantity,
    DENSE_RANK() OVER (PARTITION BY store_id, product_id ORDER BY quantity DESC) AS quantity_dense_rank
FROM inventory;
