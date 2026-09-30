SELECT status, COUNT(*) AS total_count, SUM(cost) AS total_cost FROM shipments WHERE status IN ('IN_TRANSIT', 'DELIVERED', 'LOST') GROUP BY status HAVING COUNT(*) > 1 ORDER BY total_count DESC
