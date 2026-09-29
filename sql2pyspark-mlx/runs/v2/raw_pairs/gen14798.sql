SELECT
    id,
    store_id,
    product_id,
    last_restock,
    quantity,
    ROUND(AVG(quantity) OVER (PARTITION BY store_id, product_id ORDER BY last_restock, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS quantity_moving_avg_3
FROM inventory
ORDER BY quantity_moving_avg_3 ASC
