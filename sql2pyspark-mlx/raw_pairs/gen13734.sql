SELECT status, account_type, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(account_type))) AS account_type_values FROM accounts GROUP BY status, account_type
