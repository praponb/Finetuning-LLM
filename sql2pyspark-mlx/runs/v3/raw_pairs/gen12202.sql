SELECT
    id,
    store_id,
    last_restock,
    quantity,
    LAG(quantity, 2, 0) OVER (PARTITION BY store_id ORDER BY last_restock, id) AS prev_quantity
FROM inventory
