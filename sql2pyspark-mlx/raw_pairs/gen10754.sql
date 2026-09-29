SELECT
    id,
    quantity,
    last_restock,
    store_id
FROM inventory
WHERE store_id = 5 AND quantity < 207
ORDER BY id DESC;
