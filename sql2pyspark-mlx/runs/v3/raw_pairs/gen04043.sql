SELECT ROUND((100 * SUM(CASE WHEN status = 'RETURNED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_returned, SUM(CASE WHEN status = 'RETURNED' THEN 1 ELSE 0 END) AS returned_count, ROUND(AVG(cost), 2) AS avg_cost
FROM shipments
