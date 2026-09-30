SELECT
    id,
    last_restock,
    product_id,
    store_id
FROM inventory
WHERE last_restock = (SELECT MIN(last_restock) FROM inventory WHERE product_id IN (3, 7))
