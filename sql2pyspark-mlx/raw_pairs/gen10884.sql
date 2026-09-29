SELECT
    id,
    product_id,
    quantity,
    store_id,
    last_restock,
    MIN(quantity) OVER (PARTITION BY product_id) AS product_id_min_quantity,
    LEAD(quantity, 1, 0) OVER (PARTITION BY store_id ORDER BY last_restock, id) AS next_quantity
FROM inventory
WHERE MONTH(last_restock) IN (2, 3, 12)
