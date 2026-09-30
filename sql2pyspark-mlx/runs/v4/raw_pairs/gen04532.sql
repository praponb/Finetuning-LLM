SELECT
    id,
    carrier,
    delivered_date,
    cost,
    LAG(cost, 1) OVER (PARTITION BY carrier ORDER BY delivered_date, id) AS prev_cost
FROM shipments
WHERE delivered_date <= '2023-06-01'
ORDER BY id, 5
