SELECT status, COUNT(*) AS row_count, SUM(CASE WHEN status = 'LOST' THEN cost ELSE 0 END) AS lost_cost
FROM shipments
WHERE carrier = 'Flash' OR cost < 75
GROUP BY status
ORDER BY status
