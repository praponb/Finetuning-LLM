SELECT
    id,
    product_id,
    quantity,
    store_id,
    last_restock,
    SUM(quantity) OVER (PARTITION BY product_id) AS product_id_sum_quantity,
    LEAD(quantity, 1) OVER (PARTITION BY store_id ORDER BY last_restock, id) AS next_quantity
FROM inventory
WHERE store_id IN (2, 3, 6);
