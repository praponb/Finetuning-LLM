SELECT
    id,
    carrier,
    shipped_date,
    delivered_date,
    cost,
    AVG(cost) OVER (PARTITION BY order_id, carrier ORDER BY delivered_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_cost,
    MAX(cost) OVER (PARTITION BY order_id, carrier ORDER BY delivered_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_cost,
    SUM(cost) OVER (PARTITION BY order_id, carrier ORDER BY delivered_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_cost
FROM shipments;
