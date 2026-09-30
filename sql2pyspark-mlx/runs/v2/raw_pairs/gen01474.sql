SELECT
    id,
    store_id,
    last_restock,
    product_id,
    FLOOR(quantity) AS quantity_whole
FROM inventory
WHERE quantity <= 162;
