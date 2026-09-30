SELECT
    id,
    quantity,
    last_restock,
    ROUND(quantity * 100, 2) AS adjusted_quantity
FROM inventory
WHERE store_id IN (3, 5);
