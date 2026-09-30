SELECT
    id,
    last_restock,
    product_id,
    store_id,
    DATE_FORMAT(last_restock, 'yyyy-MM-dd') AS last_restock_label
FROM inventory
WHERE product_id = 2 OR quantity <= 120
