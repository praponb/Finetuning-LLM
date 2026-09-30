SELECT
    quantity,
    store_id,
    last_restock,
    COALESCE(quantity, 0) quantity_filled
FROM inventory
ORDER BY quantity_filled DESC, store_id DESC;
