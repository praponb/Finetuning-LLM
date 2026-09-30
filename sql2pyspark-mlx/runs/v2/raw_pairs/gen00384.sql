SELECT
    id,
    product_id,
    last_restock,
    quantity,
    LAG(quantity, 1) OVER (PARTITION BY product_id ORDER BY last_restock, id) AS prev_quantity,
    LAG(quantity, 2) OVER (PARTITION BY product_id ORDER BY last_restock, id) AS prev_quantity_2
FROM inventory
WHERE quantity <> 146;
