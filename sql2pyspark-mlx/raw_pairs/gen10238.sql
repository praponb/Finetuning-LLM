SELECT
    id,
    last_restock,
    product_id,
    quantity,
    ROUND(AVG(quantity) OVER (PARTITION BY MONTH(last_restock), product_id ORDER BY last_restock, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS quantity_moving_avg_3
FROM inventory
WHERE quantity > 156;
