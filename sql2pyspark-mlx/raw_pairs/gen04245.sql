SELECT
    carrier,
    CASE WHEN cost >= 89 THEN 'high' WHEN cost >= 42 THEN 'mid' ELSE 'low' END AS cost_bucket,
    MIN(cost) AS min_cost,
    ROUND(STDDEV(cost), 2) AS stddev_cost
FROM shipments
WHERE delivered_date <= '2024-04-18'
GROUP BY carrier, CASE WHEN cost >= 89 THEN 'high' WHEN cost >= 42 THEN 'mid' ELSE 'low' END
ORDER BY min_cost DESC;
