SELECT
    store_id,
    quantity,
    product_id,
    COALESCE(quantity, 0) AS quantity_filled
FROM inventory
WHERE last_restock >= '2024-03-01'
ORDER BY quantity_filled ASC;
