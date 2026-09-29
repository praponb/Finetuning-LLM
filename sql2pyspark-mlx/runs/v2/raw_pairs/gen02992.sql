SELECT *
FROM (
    SELECT carrier, ROUND(AVG(cost), 2) AS avg_cost
    FROM shipments
    GROUP BY carrier
) AS t
WHERE avg_cost < 58;
