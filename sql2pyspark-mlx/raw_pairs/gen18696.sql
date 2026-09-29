SELECT
    id,
    store_id,
    last_restock,
    quantity,
    LAG(quantity, 1) OVER (PARTITION BY store_id ORDER BY last_restock, id) AS prev_quantity,
    SUM(quantity) OVER (ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity
FROM inventory;
