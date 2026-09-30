SELECT
    id,
    store_id,
    quantity,
    ROUND(quantity, 2) AS quantity_rounded
FROM inventory;
