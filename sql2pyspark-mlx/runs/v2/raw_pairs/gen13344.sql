SELECT a.id, COUNT(t.id) transaction_count, ROUND(STDDEV(t.amount), 2) stddev_amount
FROM accounts a
LEFT JOIN transactions t ON t.account_id = a.id
WHERE t.txn_time < '2023-11-01'
GROUP BY a.id
