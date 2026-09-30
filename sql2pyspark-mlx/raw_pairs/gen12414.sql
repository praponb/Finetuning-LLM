SELECT txn_type, SUM(CASE WHEN txn_type = 'DEBIT' THEN amount ELSE 0 END) AS debit_amount
FROM transactions
GROUP BY txn_type
