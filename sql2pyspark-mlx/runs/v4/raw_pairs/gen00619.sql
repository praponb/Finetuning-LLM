SELECT MAX(shipped_date) AS latest_shipped_date, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values FROM shipments WHERE cost BETWEEN 59 AND 80 ORDER BY latest_shipped_date;
