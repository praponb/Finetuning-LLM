SELECT
    accounts.id,
    accounts.account_type,
    accounts.opened_date,
    t.fee_count,
    t.txn_type_values
FROM accounts
JOIN (SELECT account_id, SUM(CASE WHEN txn_type = 'FEE' THEN 1 ELSE 0 END) AS fee_count, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(txn_type))) AS txn_type_values
    FROM transactions
    GROUP BY account_id) t ON accounts.id = t.account_id
ORDER BY txn_type_values, id
LIMIT 5
