SELECT
    status,
    CASE WHEN cost >= 89 THEN 'high' WHEN cost >= 35 THEN 'mid' ELSE 'low' END AS cost_bucket,
    MIN(delivered_date) AS min_delivered_date,
    ROUND((100 * SUM(CASE WHEN status = 'DELIVERED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_delivered,
    MIN(cost) AS min_cost
FROM shipments
GROUP BY 1, 2
ORDER BY min_delivered_date DESC;
