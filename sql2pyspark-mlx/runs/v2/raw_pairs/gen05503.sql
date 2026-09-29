SELECT MAX(amount) AS max_amount, SUM(CASE WHEN txn_type = 'FEE' THEN amount ELSE 0 END) AS fee_amount
FROM transactions
ORDER BY 1 DESC;
