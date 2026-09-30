SELECT carrier, status, MAX(cost) - MIN(cost) AS cost_range
FROM shipments
GROUP BY carrier, status
HAVING MAX(cost) - MIN(cost) > 66;
