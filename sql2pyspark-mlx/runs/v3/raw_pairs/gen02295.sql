SELECT t.id, t.txn_type, t.account_id
FROM transactions t
WHERE t.txn_type = 'REFUND' AND t.account_id NOT IN (SELECT id FROM accounts WHERE customer_id IS NOT NULL)
