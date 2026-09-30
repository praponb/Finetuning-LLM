SELECT
    id,
    product_id,
    last_restock,
    quantity,
    CAST(quantity AS STRING) AS quantity_num,
    DATE_ADD(last_restock, 56) AS last_restock_plus_56d
FROM inventory;
