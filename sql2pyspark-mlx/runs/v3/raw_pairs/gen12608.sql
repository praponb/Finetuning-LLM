SELECT
    a.id,
    a.status,
    a.account_type,
    a.balance
FROM accounts a
WHERE a.balance BETWEEN 51000 AND 62000 AND EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.txn_type NOT IN ('FEE', 'DEBIT'));
