SELECT
    id,
    product_id,
    last_restock,
    quantity,
    ROUND(AVG(quantity) OVER (PARTITION BY product_id ORDER BY last_restock, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS quantity_moving_avg_3
FROM inventory
WHERE store_id IN (3, 4, 5, 6)
