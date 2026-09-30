SELECT account_type, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(account_type))) account_type_values, AVG(balance) avg_balance FROM accounts GROUP BY account_type ORDER BY avg_balance ASC
