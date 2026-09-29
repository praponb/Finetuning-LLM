SELECT
    id,
    carrier,
    shipped_date,
    cost,
    SUM(cost) OVER (PARTITION BY carrier ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost
FROM shipments
