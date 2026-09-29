SELECT status, CASE WHEN status IN ('DELIVERED', 'IN_TRANSIT') THEN 'group_a' ELSE 'other' END AS status_group, MAX(cost) - MIN(cost) AS cost_range
FROM shipments
GROUP BY 1, 2
HAVING cost_range < 70
ORDER BY cost_range;
