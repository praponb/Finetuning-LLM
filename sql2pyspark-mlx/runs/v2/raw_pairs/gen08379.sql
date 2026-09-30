SELECT
    id,
    status,
    shipped_date,
    cost,
    ROUND(AVG(cost) OVER (PARTITION BY status ORDER BY shipped_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS cost_moving_avg_4
FROM shipments
WHERE carrier != 'UPS'
ORDER BY cost_moving_avg_4 DESC;
