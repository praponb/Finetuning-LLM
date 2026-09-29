SELECT DATE_TRUNC('MONTH', delivered_date) delivered_month, SUM(cost) total_cost, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) status_values
FROM shipments
WHERE MONTH(delivered_date) IN (1, 4, 10) AND delivered_date > '2024-12-01' AND status NOT IN ('RETURNED', 'IN_TRANSIT', 'LOST')
GROUP BY 1
ORDER BY total_cost DESC;
