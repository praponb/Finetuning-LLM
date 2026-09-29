SELECT carrier, status, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(carrier))) AS carrier_values FROM shipments WHERE status = 'LOST' GROUP BY carrier, status ORDER BY carrier DESC
