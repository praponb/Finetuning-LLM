SELECT
    id,
    last_restock,
    quantity,
    LEAD(quantity, 1) OVER (ORDER BY last_restock, id) AS next_quantity
FROM inventory
WHERE store_id IN (1, 2, 4, 5)
