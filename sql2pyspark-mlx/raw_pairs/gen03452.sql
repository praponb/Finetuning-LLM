SELECT
    status,
    carrier,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values,
    SUM(cost) AS total_cost
FROM shipments
GROUP BY 1, 2
ORDER BY status_values, status, carrier
LIMIT 20;
