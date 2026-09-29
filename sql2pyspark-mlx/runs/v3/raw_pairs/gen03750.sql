SELECT
    a.id,
    a.account_type,
    COUNT(t.id) transaction_count,
    MIN(t.txn_time) min_txn_time
FROM accounts a
INNER JOIN transactions t ON a.id = t.account_id
GROUP BY a.id, a.account_type;
