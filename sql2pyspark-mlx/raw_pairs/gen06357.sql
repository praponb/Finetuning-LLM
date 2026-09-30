SELECT
    carrier,
    DATE_TRUNC('MONTH', delivered_date) AS delivered_month,
    MIN(cost) AS min_cost,
    COUNT(cost) AS cost_count
FROM shipments
WHERE delivered_date > '2025-07-01' OR carrier IN ('Flash', 'Kerry', 'UPS')
GROUP BY 1, 2
