SELECT a.account_type, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(t.txn_type))) AS txn_type_values
FROM transactions t
JOIN accounts a ON t.account_id = a.id
GROUP BY a.account_type
ORDER BY a.account_type
