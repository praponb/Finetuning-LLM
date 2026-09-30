SELECT status, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values FROM shipments WHERE shipped_date > '2025-08-22' GROUP BY status ORDER BY status_values DESC, status LIMIT 5;
