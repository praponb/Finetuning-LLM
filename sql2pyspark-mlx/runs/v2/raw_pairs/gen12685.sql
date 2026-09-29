SELECT
    id,
    product_id,
    store_id,
    last_restock
FROM inventory
WHERE store_id IN (2, 4)
