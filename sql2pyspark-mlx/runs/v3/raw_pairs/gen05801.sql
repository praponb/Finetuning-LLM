SELECT
    id,
    last_restock,
    store_id,
    DATE_ADD(last_restock, 88) AS last_restock_plus_88d
FROM inventory
