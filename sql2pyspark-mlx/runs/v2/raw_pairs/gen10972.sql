SELECT
    id,
    quantity,
    store_id,
    last_restock,
    ROUND(quantity, 0) AS quantity_rounded
FROM inventory
WHERE product_id IN (6, 7);
