SELECT
    id,
    store_id,
    quantity,
    last_restock,
    COALESCE(quantity, 0) AS quantity_filled
FROM inventory
WHERE product_id = 8
