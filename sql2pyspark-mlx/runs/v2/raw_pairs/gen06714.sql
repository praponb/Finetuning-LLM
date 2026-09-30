SELECT a.id, a.status
FROM accounts a
WHERE a.customer_id = 25 AND EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.txn_type IN ('FEE', 'CREDIT', 'REFUND'))
