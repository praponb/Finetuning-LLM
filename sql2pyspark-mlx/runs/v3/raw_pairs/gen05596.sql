SELECT account_type, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values FROM accounts GROUP BY account_type ORDER BY status_values, account_type
