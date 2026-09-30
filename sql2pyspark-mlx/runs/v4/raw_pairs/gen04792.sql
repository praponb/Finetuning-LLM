SELECT
    id,
    quantity,
    last_restock,
    DATE_SUB(last_restock, 62) AS last_restock_minus_62d
FROM inventory
WHERE store_id IN (3, 4, 6) AND store_id = 2
ORDER BY id;
