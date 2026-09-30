SELECT
    a.account_type,
    COUNT(t.id) AS transaction_count,
    AVG(t.amount) AS avg_amount,
    MIN(t.txn_time) AS first_txn_time
FROM accounts a
JOIN transactions t ON a.id = t.account_id
GROUP BY a.account_type
ORDER BY avg_amount DESC;
