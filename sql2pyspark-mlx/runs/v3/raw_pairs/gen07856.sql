SELECT
    id,
    status,
    cost,
    ROUND(cost / SUM(cost) OVER (PARTITION BY status), 3) AS cost_share
FROM shipments
WHERE delivered_date < '2023-09-14'
ORDER BY status, cost_share
