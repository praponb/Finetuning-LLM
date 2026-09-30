SELECT
    carrier,
    SUM(cost) AS total_cost,
    COUNT(*) AS cnt,
    SUM(SUM(cost)) OVER (ORDER BY carrier) AS cumulative_cost
FROM shipments
WHERE delivered_date BETWEEN '2024-12-10' AND '2025-06-01'
GROUP BY carrier
