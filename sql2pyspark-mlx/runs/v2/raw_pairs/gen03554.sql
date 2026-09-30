SELECT carrier, status, SORT_ARRAY(COLLECT_SET(status)) AS status_list FROM shipments GROUP BY carrier, status ORDER BY status DESC;
