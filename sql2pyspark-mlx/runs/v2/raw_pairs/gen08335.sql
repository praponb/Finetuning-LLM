SELECT
    id,
    status,
    shipped_date,
    cost,
    SUM(cost) OVER (PARTITION BY status ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost
FROM shipments
WHERE MONTH(delivered_date) IN (8, 11, 12)
