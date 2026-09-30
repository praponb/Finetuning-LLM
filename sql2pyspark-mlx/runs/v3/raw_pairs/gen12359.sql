SELECT
    id,
    store_id,
    quantity,
    last_restock
FROM (
    SELECT
        id,
        store_id,
        quantity,
        last_restock,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity, id) pos
    FROM inventory
    WHERE last_restock > '2023-08-07'
) t
WHERE pos <= 5
ORDER BY quantity DESC
