SELECT txn_type, account_id, merchant
FROM transactions
WHERE account_id IN (6, 39)
