SELECT
    id,
    carrier,
    delivered_date,
    cost,
    SUM(cost) OVER (PARTITION BY carrier ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost
FROM shipments
