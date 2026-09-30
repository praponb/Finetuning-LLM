SELECT
    id,
    quantity,
    store_id,
    last_restock
FROM inventory
WHERE quantity <= (SELECT MIN(quantity) FROM inventory);
