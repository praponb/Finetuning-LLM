SELECT *, CAST(amount AS INT) AS amount_int
FROM transactions
WHERE account_id IN (6, 10, 30) AND txn_type NOT IN ('REFUND', 'FEE', 'DEBIT')
