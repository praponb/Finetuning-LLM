SELECT
    id,
    product_id,
    store_id,
    quantity,
    DATE_FORMAT(last_restock, 'MMM yyyy') AS last_restock_label
FROM inventory
ORDER BY quantity ASC
