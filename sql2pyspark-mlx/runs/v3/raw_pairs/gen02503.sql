SELECT
    id,
    status,
    cost,
    ROUND(cost / SUM(cost) OVER (PARTITION BY status), 3) AS cost_share
FROM shipments
WHERE delivered_date > '2024-12-01';
