SELECT
    id,
    product_id,
    last_restock,
    quantity,
    LEAD(quantity, 1) OVER (PARTITION BY product_id ORDER BY last_restock, id) AS next_quantity
FROM inventory
ORDER BY product_id, quantity DESC
