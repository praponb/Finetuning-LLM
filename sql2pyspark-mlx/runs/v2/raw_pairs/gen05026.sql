SELECT status, ROUND(AVG(cost), 2) AS avg_cost, SORT_ARRAY(COLLECT_SET(status)) AS status_list
FROM shipments
GROUP BY status
ORDER BY avg_cost DESC
