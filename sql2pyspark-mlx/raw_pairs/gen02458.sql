SELECT
    id,
    product_id,
    store_id,
    DATE_FORMAT(last_restock, 'yyyy-MM') last_restock_label
FROM inventory;
