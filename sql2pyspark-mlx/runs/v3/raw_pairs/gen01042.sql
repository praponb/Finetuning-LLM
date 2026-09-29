SELECT status, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values, COUNT(*) AS num_shipments FROM shipments GROUP BY 1 HAVING COUNT(*) < 1
