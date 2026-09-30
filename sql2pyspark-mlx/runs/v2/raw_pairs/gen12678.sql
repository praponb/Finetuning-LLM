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
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity DESC, id) AS pos
    FROM inventory
    WHERE last_restock > '2024-09-06'
) AS ranked
WHERE pos <= 3;
