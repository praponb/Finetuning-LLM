SELECT
    id,
    cost,
    shipped_date,
    status,
    ROUND(cost, 1) AS cost_rounded
FROM shipments
WHERE carrier NOT IN ('Kerry', 'Flash') OR carrier NOT IN ('Kerry', 'Flash')
ORDER BY status;
