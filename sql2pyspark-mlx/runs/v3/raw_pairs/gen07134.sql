SELECT id, status, delivered_date, cost, ROUND(AVG(cost) OVER (PARTITION BY status ORDER BY delivered_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS cost_moving_avg_3 FROM shipments;
