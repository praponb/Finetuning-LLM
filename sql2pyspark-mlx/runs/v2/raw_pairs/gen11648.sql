SELECT a.status, SORT_ARRAY(COLLECT_SET(t.txn_type)) AS txn_type_list, SORT_ARRAY(COLLECT_SET(t.merchant)) AS merchant_list
FROM transactions t
INNER JOIN accounts a ON a.id = t.account_id
GROUP BY a.status
ORDER BY status
