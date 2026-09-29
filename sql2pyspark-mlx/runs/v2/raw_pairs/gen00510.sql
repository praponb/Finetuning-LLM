SELECT
    id,
    store_id,
    last_restock,
    quantity,
    ROUND(AVG(quantity) OVER (PARTITION BY store_id ORDER BY last_restock, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) quantity_moving_avg_4
FROM inventory
ORDER BY quantity DESC
