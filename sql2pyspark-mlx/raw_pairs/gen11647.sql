SELECT
    carrier,
    status,
    total_cost,
    min_cost
FROM (
    SELECT
        carrier,
        status,
        SUM(cost) AS total_cost,
        MIN(cost) AS min_cost
    FROM shipments
    WHERE shipped_date > '2024-11-01'
    GROUP BY carrier, status
) AS x
WHERE min_cost < 90
ORDER BY min_cost DESC
