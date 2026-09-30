SELECT
    id,
    delivered_date,
    cost,
    SUM(cost) OVER (ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost
FROM shipments
ORDER BY id;
