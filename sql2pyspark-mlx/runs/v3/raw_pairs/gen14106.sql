SELECT a.account_type, t.txn_type_list, t.unique_merchants
FROM accounts a
JOIN (SELECT account_id, SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list, COUNT(DISTINCT amount) AS unique_merchants
    FROM transactions
    GROUP BY account_id) t ON a.id = t.account_id
WHERE t.unique_merchants >= 5
ORDER BY a.account_type ASC;
