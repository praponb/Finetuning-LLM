SELECT *
FROM (
    SELECT CASE WHEN cost >= 83 THEN 'high' WHEN cost >= 26 THEN 'mid' ELSE 'low' END AS cost_bucket, MAX(cost) - MIN(cost) AS cost_range, ROUND(AVG(cost), 2) AS avg_cost
    FROM shipments
    GROUP BY CASE WHEN cost >= 83 THEN 'high' WHEN cost >= 26 THEN 'mid' ELSE 'low' END
) AS s
WHERE avg_cost < 41
ORDER BY cost_bucket DESC;
