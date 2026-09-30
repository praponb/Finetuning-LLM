SELECT account_type, SORT_ARRAY(COLLECT_SET(account_type)) AS account_type_list FROM accounts GROUP BY account_type
