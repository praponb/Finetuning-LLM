SELECT
    id,
    carrier,
    order_id,
    shipped_date,
    cost,
    ROUND(AVG(cost) OVER (PARTITION BY carrier, order_id ORDER BY shipped_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS cost_moving_avg_4
FROM shipments
ORDER BY shipped_date DESC;
