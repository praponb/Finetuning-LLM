SELECT status, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(carrier))) AS carrier_values, SUM(cost) AS total_cost
FROM shipments
GROUP BY status
ORDER BY status DESC;
