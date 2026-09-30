SELECT ROUND((100 * SUM(CASE WHEN status = 'RETURNED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_returned
FROM shipments
ORDER BY pct_returned DESC
