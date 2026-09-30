SELECT c.segment, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(a.status))) AS status_values FROM accounts a JOIN customers c ON a.customer_id = c.id GROUP BY c.segment ORDER BY status_values;
