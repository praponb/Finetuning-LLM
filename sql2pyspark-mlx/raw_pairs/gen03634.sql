SELECT
    id,
    store_id,
    last_restock,
    product_id,
    quantity,
    DENSE_RANK() OVER (PARTITION BY store_id ORDER BY last_restock DESC) AS last_restock_dense_rank,
    LAG(quantity, 1) OVER (PARTITION BY product_id ORDER BY last_restock, id) AS prev_quantity
FROM inventory
WHERE quantity IS NOT NULL;
