SELECT
    status,
    MIN(cost) min_cost,
    MIN(delivered_date) min_delivered_date,
    ROUND(AVG(cost), 2) avg_cost
FROM shipments
WHERE delivered_date < '2025-08-01' AND delivered_date < '2024-03-10' AND YEAR(delivered_date) >= 2025
GROUP BY status
ORDER BY status
LIMIT 20;
