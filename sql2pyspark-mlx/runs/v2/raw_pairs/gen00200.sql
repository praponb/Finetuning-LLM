SELECT
    id,
    product_id,
    last_restock,
    quantity,
    quantity - LAG(quantity, 2) OVER (PARTITION BY product_id ORDER BY last_restock, id) quantity_change
FROM inventory
WHERE product_id IN (2, 4, 7, 8)
