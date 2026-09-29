SELECT status, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(carrier))) AS carrier_values
FROM shipments
WHERE cost > 44
GROUP BY status
ORDER BY status_values, carrier_values;
