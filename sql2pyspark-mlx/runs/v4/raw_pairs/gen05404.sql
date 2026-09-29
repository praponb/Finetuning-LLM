SELECT
    id,
    last_restock,
    product_id,
    quantity
FROM inventory
WHERE last_restock < (SELECT MIN(last_restock) FROM inventory WHERE quantity > 230)
