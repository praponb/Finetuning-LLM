SELECT status, COUNT(cost) AS cost_count, MIN(shipped_date) AS min_shipped_date
FROM shipments
WHERE carrier NOT IN ('DHL', 'UPS')
GROUP BY status
