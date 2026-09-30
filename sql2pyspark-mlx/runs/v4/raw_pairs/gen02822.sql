SELECT
    id,
    order_id,
    quantity,
    unit_price,
    ROUND(AVG(unit_price) OVER (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS unit_price_moving_avg_4
FROM order_items
WHERE order_id IN (10, 11, 30, 33)
ORDER BY quantity DESC;
