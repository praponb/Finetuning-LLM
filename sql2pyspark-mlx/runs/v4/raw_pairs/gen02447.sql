SELECT MAX(cost) - MIN(cost) cost_range, MAX(shipped_date) latest_shipped_date, COUNT(cost) cost_count
FROM shipments
ORDER BY latest_shipped_date
