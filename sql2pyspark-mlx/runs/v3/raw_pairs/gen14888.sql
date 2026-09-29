SELECT
    id,
    product_id,
    last_restock,
    quantity,
    LEAD(quantity, 1) OVER (PARTITION BY product_id ORDER BY last_restock, id) next_quantity
FROM inventory
WHERE product_id IN (3, 6, 7)
