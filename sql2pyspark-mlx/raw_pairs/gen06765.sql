SELECT
    id,
    product_id,
    order_id,
    quantity,
    ROUND(AVG(quantity) OVER (PARTITION BY product_id, order_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS quantity_moving_avg_3
FROM order_items;
