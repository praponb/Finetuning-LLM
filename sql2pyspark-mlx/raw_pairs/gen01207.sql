SELECT carrier, MAX(cost) AS max_cost
FROM shipments
GROUP BY carrier
HAVING MAX(cost) >= 75
ORDER BY max_cost DESC;
