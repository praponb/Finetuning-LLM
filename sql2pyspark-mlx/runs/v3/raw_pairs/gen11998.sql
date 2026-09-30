SELECT
    id,
    quantity,
    last_restock,
    store_id,
    COALESCE(quantity, 0) AS quantity_filled
FROM inventory
WHERE product_id IN (5, 7, 10) AND product_id IN (1, 6, 7, 8) AND last_restock >= '2024-03-29'
