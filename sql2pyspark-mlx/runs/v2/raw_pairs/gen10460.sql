SELECT
    id,
    carrier,
    delivered_date,
    cost,
    cost - LAG(cost, 1) OVER (PARTITION BY carrier ORDER BY delivered_date, id) AS cost_change
FROM shipments
ORDER BY cost DESC;
