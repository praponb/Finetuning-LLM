SELECT
    id,
    product_id,
    last_restock,
    quantity,
    LAG(quantity, 2, 0) OVER (PARTITION BY product_id ORDER BY last_restock, id) AS prev_quantity
FROM inventory
WHERE store_id IN (2, 3, 5, 6);
