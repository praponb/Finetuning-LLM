SELECT
    carrier,
    COUNT(*) AS cnt,
    ROUND(AVG(cost), 2) AS avg_cost,
    ROUND(STDDEV(cost), 2) AS stddev_cost
FROM shipments
WHERE status = 'LOST'
GROUP BY 1
HAVING cnt > 3
