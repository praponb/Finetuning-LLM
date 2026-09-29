SELECT a.account_type, COUNT(t.id) AS transaction_count, SUM(CASE WHEN t.txn_type = 'FEE' THEN t.amount ELSE 0 END) AS fee_amount
FROM accounts a
LEFT OUTER JOIN transactions t ON t.account_id = a.id
WHERE t.merchant IN ('Lazada', '7-Eleven')
GROUP BY a.account_type
ORDER BY account_type
