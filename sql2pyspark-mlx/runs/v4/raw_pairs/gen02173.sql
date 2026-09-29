SELECT
    id,
    store_id,
    last_restock,
    quantity,
    ROUND(AVG(quantity) OVER (PARTITION BY store_id ORDER BY last_restock, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) quantity_moving_avg_4,
    SUM(quantity) OVER (PARTITION BY store_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_quantity
FROM inventory
WHERE last_restock < '2025-01-01';
