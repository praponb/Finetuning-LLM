SELECT
    a.id,
    a.balance,
    a.status,
    a.customer_id
FROM accounts a
WHERE a.balance < 38000 AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.txn_type IN ('DEBIT', 'CREDIT', 'REFUND'))
ORDER BY a.status DESC, id
LIMIT 5;
