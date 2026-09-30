SELECT
    a.id,
    a.status,
    COUNT(t.id) transaction_count,
    SORT_ARRAY(COLLECT_SET(t.txn_type)) txn_type_list
FROM accounts a
INNER JOIN transactions t ON t.account_id = a.id
GROUP BY a.id, a.status;
