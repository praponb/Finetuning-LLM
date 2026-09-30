SELECT
    carrier,
    status,
    AVG(cost) AS avg_cost,
    SUM(CASE WHEN status = 'DELIVERED' THEN 1 ELSE 0 END) AS delivered_count
FROM shipments
WHERE cost BETWEEN 54 AND 65
GROUP BY carrier, status
ORDER BY avg_cost DESC, carrier, status
LIMIT 10;
