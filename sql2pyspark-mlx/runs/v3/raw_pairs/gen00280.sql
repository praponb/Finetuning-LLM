SELECT
    id,
    store_id,
    last_restock,
    RANK() OVER (PARTITION BY store_id ORDER BY last_restock) AS last_restock_rank
FROM inventory;
