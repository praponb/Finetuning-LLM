SELECT
    id,
    quantity,
    last_restock,
    store_id
FROM inventory
WHERE last_restock > '2025-02-01';
