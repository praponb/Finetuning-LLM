SELECT status, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(account_type))) AS account_type_values, ROUND(STDDEV(balance), 2) AS stddev_balance
FROM accounts
GROUP BY status
