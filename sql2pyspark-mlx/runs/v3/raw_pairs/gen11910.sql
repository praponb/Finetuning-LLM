SELECT
    id,
    status,
    delivered_date,
    cost,
    LAG(cost, 1) OVER (PARTITION BY status ORDER BY delivered_date, id) AS prev_cost
FROM shipments
WHERE shipped_date < '2024-01-06'
