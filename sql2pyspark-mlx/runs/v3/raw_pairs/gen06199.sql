SELECT
    a.id,
    a.account_type,
    COUNT(t.id) AS transaction_count,
    MIN(t.txn_time) AS min_txn_time
FROM accounts a
LEFT JOIN transactions t ON t.account_id = a.id
GROUP BY a.id, a.account_type
ORDER BY min_txn_time, id, account_type
LIMIT 5
