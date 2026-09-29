SELECT a.account_type, SUM(CASE WHEN t.txn_type = 'DEBIT' THEN t.amount ELSE 0 END) AS debit_amount
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
WHERE t.is_flagged = TRUE
GROUP BY a.account_type
ORDER BY debit_amount DESC, account_type DESC
LIMIT 5;
