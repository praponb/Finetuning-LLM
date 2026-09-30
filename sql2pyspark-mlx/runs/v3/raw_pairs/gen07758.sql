SELECT *
FROM (
    SELECT carrier, ROUND(STDDEV(cost), 2) AS stddev_cost, COUNT(DISTINCT cost) AS unique_orders
    FROM shipments
    GROUP BY carrier
) AS agg
WHERE stddev_cost > 47
ORDER BY carrier, 3;
