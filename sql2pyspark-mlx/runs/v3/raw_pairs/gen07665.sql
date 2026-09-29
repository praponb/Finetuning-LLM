SELECT SUM(CASE WHEN txn_type = 'CREDIT' THEN amount ELSE 0 END) AS credit_amount
FROM transactions
WHERE txn_type = 'FEE' AND amount BETWEEN 3400 AND 3700
