SELECT
    id,
    last_restock,
    quantity,
    store_id
FROM inventory
WHERE store_id IN (1, 3, 4, 6)
ORDER BY store_id DESC
