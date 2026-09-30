SELECT carrier, AVG(cost) AS avg_cost
FROM shipments
GROUP BY carrier
ORDER BY avg_cost, 1 DESC
LIMIT 10;
