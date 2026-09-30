SELECT channel, status, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values FROM orders GROUP BY channel, status ORDER BY status DESC
