SELECT a.status, COUNT(t.id) AS transaction_count, SORT_ARRAY(COLLECT_SET(t.txn_type)) AS txn_type_list
FROM accounts a
LEFT JOIN transactions t ON a.id = t.account_id
GROUP BY a.status;
