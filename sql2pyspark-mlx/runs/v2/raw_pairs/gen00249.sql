SELECT account_type, customer_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) status_values
FROM accounts
GROUP BY CUBE(account_type, customer_id)
ORDER BY status_values
