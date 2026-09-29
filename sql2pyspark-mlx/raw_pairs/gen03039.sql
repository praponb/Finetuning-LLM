SELECT
    id,
    quantity,
    store_id,
    last_restock,
    DATE_FORMAT(last_restock, 'yyyy-MM') AS last_restock_label
FROM inventory
ORDER BY last_restock
