SELECT
    id,
    carrier,
    cost,
    delivered_date,
    ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost DESC, id) AS rn,
    ROUND(AVG(cost) OVER (PARTITION BY carrier ORDER BY delivered_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS cost_moving_avg_4
FROM shipments
WHERE order_id IN (19, 22, 31);
