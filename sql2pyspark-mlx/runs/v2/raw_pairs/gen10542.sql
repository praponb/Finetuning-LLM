SELECT
    id,
    quantity,
    product_id,
    store_id,
    DATE_FORMAT(last_restock, 'MMM yyyy') AS last_restock_label
FROM inventory;
