SELECT status, COUNT(cost) AS cost_count, MIN(shipped_date) AS first_shipped_date
FROM shipments
GROUP BY status
HAVING COUNT(cost) < 2;
