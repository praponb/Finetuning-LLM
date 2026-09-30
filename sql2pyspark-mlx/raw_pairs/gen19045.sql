SELECT
    id,
    store_id,
    last_restock,
    product_id
FROM inventory
WHERE product_id IN (2, 4, 6);
