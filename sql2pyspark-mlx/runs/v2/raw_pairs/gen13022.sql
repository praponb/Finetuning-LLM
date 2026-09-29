SELECT status, COUNT(DISTINCT cost) AS unique_carriers, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values FROM shipments GROUP BY status ORDER BY status DESC;
