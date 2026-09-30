SELECT COUNT(*) AS row_count, ROUND(AVG(cost), 2) AS avg_cost
FROM shipments
ORDER BY 2, row_count DESC;
