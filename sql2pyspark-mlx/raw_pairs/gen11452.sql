SELECT account_type, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(account_type))) AS account_type_values FROM accounts GROUP BY account_type ORDER BY account_type
