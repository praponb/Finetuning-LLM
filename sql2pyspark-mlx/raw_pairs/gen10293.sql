SELECT
    last_restock,
    store_id,
    quantity,
    CAST(quantity AS DOUBLE) AS quantity_str
FROM inventory
WHERE quantity != 122 AND store_id IN (1, 4);
