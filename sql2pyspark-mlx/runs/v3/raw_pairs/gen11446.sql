SELECT carrier, COUNT(cost) AS cost_count
FROM shipments
GROUP BY carrier
ORDER BY carrier;
