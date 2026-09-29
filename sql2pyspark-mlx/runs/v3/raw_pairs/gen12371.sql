SELECT
    id,
    shipped_date,
    cost,
    SUM(cost) OVER (ORDER BY shipped_date, id) AS running_cost
FROM shipments
ORDER BY cost DESC, id DESC;
