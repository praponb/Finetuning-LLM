SELECT
    id,
    store_id,
    last_restock,
    quantity,
    DENSE_RANK() OVER (PARTITION BY store_id ORDER BY last_restock) AS last_restock_dense_rank,
    SUM(quantity) OVER (PARTITION BY store_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity
FROM inventory
ORDER BY 3, store_id DESC
