SELECT
    carrier,
    SUM(cost) AS total_cost,
    COUNT(*) AS cnt,
    SUM(SUM(cost)) OVER (ORDER BY carrier) AS cumulative_cost
FROM shipments
WHERE shipped_date BETWEEN '2023-11-23' AND '2024-10-01'
GROUP BY carrier
ORDER BY total_cost;
