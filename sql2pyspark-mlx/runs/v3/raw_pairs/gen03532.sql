SELECT
    status,
    SUM(cost) AS total_cost,
    COUNT(*) AS cnt,
    SUM(SUM(cost)) OVER (ORDER BY status) AS cumulative_cost
FROM shipments
WHERE cost >= 37
GROUP BY status;
