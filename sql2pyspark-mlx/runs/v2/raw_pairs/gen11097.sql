SELECT MAX(cost) - MIN(cost) AS cost_range, AVG(cost) AS avg_cost, SORT_ARRAY(COLLECT_SET(carrier)) AS carrier_list
FROM shipments
ORDER BY cost_range;
