SELECT MIN(shipped_date) AS min_shipped_date, COUNT(*) AS total_count, ROUND(AVG(cost), 2) AS avg_cost
FROM shipments
WHERE cost IS NULL
ORDER BY 1 DESC;
