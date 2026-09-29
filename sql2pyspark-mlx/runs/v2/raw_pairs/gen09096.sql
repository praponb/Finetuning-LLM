SELECT
    id,
    store_id,
    quantity,
    NTILE(4) OVER (PARTITION BY store_id ORDER BY quantity, id) AS quantity_ntile
FROM inventory;
