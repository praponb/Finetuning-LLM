SELECT account_id, merchant, txn_type, SUM(CASE WHEN txn_type = 'DEBIT' THEN amount ELSE 0 END) AS debit_amount FROM transactions WHERE account_id IN (28, 30) GROUP BY account_id, merchant, txn_type;
