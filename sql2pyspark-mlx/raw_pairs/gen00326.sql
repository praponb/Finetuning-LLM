SELECT
    id,
    quantity,
    last_restock,
    LAG(quantity) OVER (PARTITION BY store_id ORDER BY quantity, id) AS prev_quantity,
    ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity, id) AS seq_no
FROM inventory;
