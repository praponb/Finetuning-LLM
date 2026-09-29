SELECT
    id,
    last_restock,
    store_id,
    ROUND(quantity * 1.1, 2) AS quantity_scaled
FROM inventory
WHERE product_id IN (3, 9) AND (store_id IN (1, 4, 5, 6) OR product_id = 7);
