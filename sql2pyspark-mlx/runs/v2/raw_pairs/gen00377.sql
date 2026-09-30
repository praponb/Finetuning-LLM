SELECT
    id,
    quantity,
    store_id,
    last_restock
FROM inventory
WHERE quantity > (SELECT MAX(quantity) FROM inventory WHERE quantity <> 163)
ORDER BY quantity DESC
