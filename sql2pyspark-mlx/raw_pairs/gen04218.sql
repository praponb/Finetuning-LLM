SELECT
    id,
    product_id,
    last_restock,
    quantity,
    store_id,
    SUM(quantity) OVER (PARTITION BY product_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity,
    ROUND(AVG(quantity) OVER (PARTITION BY product_id, store_id ORDER BY last_restock, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS quantity_moving_avg_3
FROM inventory
