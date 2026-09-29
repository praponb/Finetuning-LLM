SELECT
    a.id,
    a.balance,
    a.account_type,
    a.status
FROM accounts a
WHERE a.balance > 39000 AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.txn_type IN ('CREDIT', 'FEE'))
