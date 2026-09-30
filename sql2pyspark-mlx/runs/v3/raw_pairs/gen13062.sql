SELECT
    id,
    product_id,
    last_restock,
    quantity,
    LAG(quantity, 1) OVER (PARTITION BY product_id ORDER BY last_restock, id) AS prev_quantity
FROM inventory
ORDER BY product_id DESC
