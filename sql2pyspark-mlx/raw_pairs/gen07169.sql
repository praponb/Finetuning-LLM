SELECT
    id,
    product_id,
    quantity,
    last_restock,
    MAX(quantity) OVER (PARTITION BY product_id ORDER BY last_restock, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_quantity,
    SUM(quantity) OVER (PARTITION BY product_id ORDER BY last_restock, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_quantity,
    COUNT(quantity) OVER (PARTITION BY product_id ORDER BY last_restock, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_quantity,
    MIN(quantity) OVER (PARTITION BY product_id ORDER BY last_restock, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_quantity
FROM inventory
WHERE store_id IN (1, 3, 4, 6);
