SELECT
    id,
    store_id,
    product_id,
    last_restock,
    quantity,
    COUNT(*) OVER (PARTITION BY store_id) AS store_id_count,
    quantity - LAG(quantity, 1) OVER (PARTITION BY product_id ORDER BY last_restock, id) AS quantity_change
FROM inventory
WHERE product_id IN (3, 5, 6);
