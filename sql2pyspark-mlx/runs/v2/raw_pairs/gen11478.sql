SELECT
    id,
    store_id,
    last_restock,
    quantity,
    LEAD(quantity, 1) OVER (PARTITION BY store_id ORDER BY last_restock, id) next_quantity
FROM inventory
WHERE store_id = 5;
