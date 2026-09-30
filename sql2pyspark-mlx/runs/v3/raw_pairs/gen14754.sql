SELECT status, carrier, MAX(cost) - MIN(cost) AS cost_range
FROM shipments
GROUP BY status, carrier
HAVING COUNT(*) > 4;
