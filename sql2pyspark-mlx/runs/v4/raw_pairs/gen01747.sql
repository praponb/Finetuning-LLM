SELECT
    id,
    carrier,
    shipped_date,
    cost,
    cost - LAG(cost, 1) OVER (PARTITION BY carrier ORDER BY shipped_date, id) AS cost_change,
    ROUND(cost / SUM(cost) OVER (PARTITION BY carrier), 3) AS cost_share
FROM shipments
