SELECT
    id,
    store_id,
    last_restock,
    quantity,
    SUM(quantity) OVER (PARTITION BY store_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_quantity,
    ROW_NUMBER() OVER (ORDER BY last_restock, id) last_restock_row
FROM inventory
WHERE product_id IN (2, 5, 8, 10)
ORDER BY quantity DESC
