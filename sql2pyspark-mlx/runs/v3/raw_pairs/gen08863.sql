SELECT
    carrier,
    COUNT(CASE WHEN status = 'DELIVERED' THEN 1 END) AS delivered_count,
    MAX(delivered_date) AS latest_delivered_date,
    ROUND(AVG(cost), 2) AS avg_cost
FROM shipments
GROUP BY carrier
ORDER BY delivered_count DESC
