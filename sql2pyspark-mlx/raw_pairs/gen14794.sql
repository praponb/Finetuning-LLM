SELECT
    a.id,
    a.account_type,
    COUNT(t.id) AS transaction_count,
    AVG(t.amount) AS avg_amount,
    COUNT(CASE WHEN t.txn_type = 'CREDIT' THEN 1 END) AS credit_count
FROM accounts a
INNER JOIN transactions t ON a.id = t.account_id
GROUP BY a.id, a.account_type
