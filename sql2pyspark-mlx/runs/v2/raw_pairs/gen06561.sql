SELECT
    id,
    product_id,
    store_id,
    last_restock,
    quantity,
    LEAD(quantity, 1) OVER (PARTITION BY product_id, store_id ORDER BY last_restock, id) AS next_quantity
FROM inventory
ORDER BY id DESC;
