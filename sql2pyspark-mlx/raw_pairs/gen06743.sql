SELECT
    id,
    product_id,
    store_id,
    last_restock,
    quantity,
    ROUND(AVG(quantity) OVER (PARTITION BY product_id, store_id ORDER BY last_restock, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS quantity_moving_avg_3,
    LAG(quantity, 2, 0) OVER (ORDER BY last_restock, id) AS prev_quantity
FROM inventory;
