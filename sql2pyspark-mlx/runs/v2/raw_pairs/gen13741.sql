SELECT status, SORT_ARRAY(COLLECT_SET(carrier)) AS carrier_list, MAX(cost) - MIN(cost) AS cost_range
FROM shipments
WHERE carrier IN ('Flash', 'DHL', 'FedEx')
GROUP BY 1;
