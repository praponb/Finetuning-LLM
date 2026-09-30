SELECT
    carrier,
    CASE WHEN cost >= 95 THEN 'high' WHEN cost >= 30 THEN 'mid' ELSE 'low' END AS cost_bucket,
    ROUND(STDDEV(cost), 2) AS stddev_cost,
    MAX(cost) AS max_cost
FROM shipments
GROUP BY carrier, CASE WHEN cost >= 95 THEN 'high' WHEN cost >= 30 THEN 'mid' ELSE 'low' END;
