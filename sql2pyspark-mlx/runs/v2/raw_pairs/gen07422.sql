SELECT status, MIN(cost) AS min_cost, SUM(CASE WHEN status = 'LOST' THEN 1 ELSE 0 END) AS lost_count
FROM shipments
GROUP BY status
