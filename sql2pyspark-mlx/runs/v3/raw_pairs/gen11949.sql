SELECT carrier, COUNT(cost) AS cost_count
FROM shipments
WHERE carrier = 'Flash'
GROUP BY carrier
