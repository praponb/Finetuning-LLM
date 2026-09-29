SELECT
    carrier,
    SUM(CASE WHEN status = 'LOST' THEN cost ELSE 0 END) AS lost_cost,
    SUM(cost) AS total_cost,
    ROUND(STDDEV(cost), 2) AS stddev_cost
FROM shipments
WHERE carrier NOT IN ('DHL', 'Kerry', 'Flash')
GROUP BY carrier
HAVING SUM(CASE WHEN status = 'LOST' THEN cost ELSE 0 END) < 45
