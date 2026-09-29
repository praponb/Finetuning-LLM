SELECT
    id,
    store_id,
    quantity,
    last_restock,
    NTILE(4) OVER (PARTITION BY store_id ORDER BY quantity, id) AS bucket,
    ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock, id) AS rn
FROM inventory
