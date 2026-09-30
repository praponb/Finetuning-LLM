SELECT
    id,
    store_id,
    last_restock,
    quantity,
    LEAD(quantity, 1, 0) OVER (PARTITION BY store_id ORDER BY last_restock, id) next_quantity
FROM inventory
WHERE quantity < 120
ORDER BY store_id;
