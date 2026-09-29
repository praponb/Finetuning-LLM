SELECT
    id,
    order_id,
    delivered_date,
    cost,
    ROUND(AVG(cost) OVER (PARTITION BY order_id ORDER BY delivered_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS cost_moving_avg_7
FROM shipments
WHERE status != 'RETURNED';
