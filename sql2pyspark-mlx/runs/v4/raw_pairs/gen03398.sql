SELECT transactions.id, transactions.merchant, transactions.txn_type FROM transactions WHERE transactions.account_id IN (SELECT id FROM accounts WHERE status IN ('FROZEN', 'CLOSED'))
