SELECT
    id,
    quantity,
    store_id,
    last_restock
FROM inventory
WHERE store_id IN (3, 5);
