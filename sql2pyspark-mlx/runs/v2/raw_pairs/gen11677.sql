SELECT *
FROM transactions
WHERE account_id IN (3, 30) AND txn_type IN ('CREDIT', 'FEE') AND txn_type = 'FEE'
