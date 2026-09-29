SELECT
    carrier,
    status,
    COUNT(CASE WHEN carrier = 'DHL' THEN 1 END) AS dhl_count,
    ROUND(STDDEV(cost), 2) AS stddev_cost,
    ROUND(AVG(cost), 2) AS avg_cost
FROM shipments
WHERE shipped_date > '2025-08-01'
GROUP BY carrier, status
ORDER BY avg_cost
