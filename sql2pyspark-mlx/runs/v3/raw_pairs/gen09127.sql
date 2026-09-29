SELECT
    a.account_type,
    a.status,
    MIN(t.txn_time) AS first_txn_time,
    ROUND(AVG(t.amount), 2) AS avg_amount
FROM transactions t
JOIN accounts a ON t.account_id = a.id
GROUP BY a.account_type, a.status
