SELECT a.account_type, SUM(CASE WHEN t.txn_type = 'DEBIT' THEN 1 ELSE 0 END) AS debit_count
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
INNER JOIN customers c ON a.customer_id = c.id
WHERE c.country IN ('SG', 'TH')
GROUP BY a.account_type
ORDER BY a.account_type DESC;
