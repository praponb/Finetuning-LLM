SELECT
    id,
    product_id,
    store_id,
    last_restock,
    CAST(quantity AS STRING) AS quantity_num,
    DATE_FORMAT(last_restock, 'yyyy-MM-dd') AS last_restock_label
FROM inventory
WHERE product_id IN (1, 7, 10) AND product_id IN (1, 3, 8);
