SELECT a.account_type, AVG(t.amount) AS avg_amount, COUNT(CASE WHEN t.txn_type = 'FEE' THEN 1 END) AS fee_count
FROM transactions t
LEFT JOIN accounts a ON t.account_id = a.id
LEFT JOIN customers c ON a.customer_id = c.id
WHERE t.account_id IN (14, 26, 30)
GROUP BY a.account_type
ORDER BY account_type
