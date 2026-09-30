SELECT CASE WHEN status IN ('LOST', 'IN_TRANSIT') THEN 'core' ELSE 'other' END AS status_group, MAX(cost) - MIN(cost) AS cost_range, ROUND(AVG(cost), 2) AS avg_cost
FROM shipments
GROUP BY 1
HAVING MAX(cost) - MIN(cost) < 49
ORDER BY cost_range DESC
