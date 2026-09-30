SELECT
    id,
    store_id,
    quantity,
    ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity DESC, id) AS quantity_row
FROM inventory
ORDER BY id DESC, quantity_row
