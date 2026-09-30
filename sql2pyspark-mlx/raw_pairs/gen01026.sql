SELECT
    product_id,
    store_id,
    MIN(quantity) AS min_quantity,
    AVG(quantity) AS avg_quantity
FROM inventory
WHERE last_restock <= '2024-09-05' AND quantity != 126
GROUP BY product_id, store_id;
